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
from pathlib import Path

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

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
    raise NotImplementedError("Step 5: publish the models to Iceberg")


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
