"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import uuid
from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import _dataframe_to_data_files
from pyiceberg.table import Table

DBT_DIR = Path(__file__).resolve().parent / "dbt"


def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}", warehouse=warehouse.resolve().as_posix())


def ingest(data_dir: Path, db: Path) -> None:
    """Step 1: land the sources in DuckDB, schema `raw`, exactly as they arrive.

    - From data_dir/qafila_oltp.sqlite: orders, order_items, products, stores.
    - Every data_dir/suppliers/prices_*.csv, keeping the file name of each row.

    Running this twice must not duplicate anything.
    """
    sqlite_path = (data_dir / "qafila_oltp.sqlite").resolve().as_posix()
    supplier_glob = (data_dir / "suppliers" / "prices_*.csv").resolve().as_posix()

    with duckdb.connect(str(db)) as con:  # closes the file before dbt opens it
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")

        # App database: attach it read-only and copy the four tables as they are (Lab 1.1).
        # CREATE OR REPLACE makes the load idempotent: a re-run replaces, never appends.
        con.sql(f"ATTACH '{sqlite_path}' AS app (TYPE sqlite, READ_ONLY)")
        for table in ["orders", "order_items", "products", "stores"]:
            con.sql(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM app.{table}")
        con.sql("DETACH app")

        # Supplier files: every column as text, so '9,49' and Arabic-Indic digits arrive untouched
        # (parsing is staging's job, Lab 2.4). union_by_name lines up W39's renamed/reordered columns
        # by name (unit_cost stays NULL there, cost_sar appears). filename keeps each row's source.
        con.sql(f"""
            CREATE OR REPLACE TABLE raw.supplier_prices AS
            SELECT * EXCLUDE (filename),
                   parse_filename(filename) AS source_file
            FROM read_csv('{supplier_glob}', all_varchar = true, union_by_name = true,
                          filename = true, header = true)
        """)
        n_files, n_rows = con.sql(
            "SELECT count(DISTINCT source_file), count(*) FROM raw.supplier_prices").fetchone()
        print(f"ingest: {n_rows} supplier rows from {n_files} files")


def transform(db: Path) -> None:
    """Steps 2-4: build and test the dbt project in dbt/ against the DuckDB file.

    dbt runs in its own process: in this one it would keep the DuckDB file open and block publish().
    """
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",
           "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


def publish(db: Path, warehouse: Path) -> None:
    """Step 5: publish three dbt models to Iceberg (see the brief for the exact columns).

        silver.product_cost_history   gold.weekly_margin   quality.price_quarantine

    Update existing tables in place, never drop and recreate them: the table's snapshots are
    how we see what a number was before the supplier's correction arrived.
    """
    catalog = get_catalog(warehouse)
    with duckdb.connect(str(db), read_only=True) as con:
        new_data = {name: con.sql(query).to_arrow_table() for name, query in PUBLISHED_TABLES.items()}

    for name, data in new_data.items():
        catalog.create_namespace_if_not_exists(name.split(".")[0])
        if catalog.table_exists(name):
            table = catalog.load_table(name)
            with table.update_schema() as schema:  # new columns are added, nothing is dropped
                schema.union_by_name(data.schema)
        else:
            table = catalog.create_table(name, schema=data.schema)

        if same_rows(table.scan().to_arrow(), data):
            print(f"publish: {name} unchanged ({data.num_rows} rows), no new snapshot")
            continue
        replace_rows(table, data)
        print(f"publish: {name} -> {data.num_rows} rows, snapshot {table.current_snapshot().snapshot_id}")


# The three published tables: the contract's columns, in the contract's order.
PUBLISHED_TABLES = {
    "silver.product_cost_history": """
        SELECT sku, unit_cost_sar, valid_from, valid_to, is_current
        FROM silver.product_cost_history ORDER BY sku, valid_from""",
    "gold.weekly_margin": """
        SELECT iso_week, city, category, lines, revenue_sar, costed_revenue_sar, cost_sar, margin_sar,
               uncosted_lines
        FROM gold.weekly_margin ORDER BY iso_week, city, category""",
    "quality.price_quarantine": """
        SELECT source_file, sku, valid_from, raw_cost, reason
        FROM quality.price_quarantine ORDER BY source_file, sku, valid_from""",
}


def replace_rows(table: Table, data: pa.Table) -> None:
    """Replace all rows of an Iceberg table in ONE snapshot, keeping the older snapshots readable.

    table.overwrite() commits two snapshots (a delete that leaves the table empty, then an append),
    so the snapshot before the current one would be an empty table. Here the old data files are
    removed and the new ones added in a single OVERWRITE snapshot (APPEND on the first write).
    """
    commit_uuid = uuid.uuid4()
    with table.transaction() as tx:
        new_files = _dataframe_to_data_files(table_metadata=tx.table_metadata, write_uuid=commit_uuid,
                                             df=data, io=table.io)
        with tx.update_snapshot().overwrite(commit_uuid=commit_uuid) as snapshot:
            for task in table.scan().plan_files():
                snapshot.delete_data_file(task.file)
            for data_file in new_files:
                snapshot.append_data_file(data_file)


def same_rows(current: pa.Table, new: pa.Table) -> bool:
    """True when the Iceberg table already holds exactly these rows (order ignored)."""
    if current.num_rows != new.num_rows or set(current.column_names) != set(new.column_names):
        return False
    cols = new.column_names
    a = pl.from_arrow(current).select(cols).sort(cols, nulls_last=True)
    b = pl.from_arrow(new).select(cols).sort(cols, nulls_last=True)
    return a.equals(b)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
