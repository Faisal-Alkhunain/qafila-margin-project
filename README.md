# Qafila gross margin

Submitted by: <your name> — academy: @SDAIAAcademy

The task, rules and grading are in [BRIEF.md](BRIEF.md).

```bash
uv sync
uv run python tools/make_data.py          # data/
uv run python run.py                      # data/ -> warehouse/ (DuckDB + Iceberg)
uv run python tools/check.py              # contract self-check
```

## Design

**Flow:** supplier CSVs + app SQLite → `raw` (DuckDB, `run.py: ingest`) → `staging` → `intermediate` → `silver` / `quality` → `gold` (dbt-duckdb, `dbt build`) → Iceberg tables `silver.*`, `gold.*`, `quality.*` (PyIceberg, `run.py: publish`).

| Model | Layer | Grain (one row per ...) | Materialization |
|---|---|---|---|
| `raw.orders`, `order_items`, `products`, `stores` | raw | source row, copied as is from SQLite | table (`ingest`) |
| `raw.supplier_prices` | raw | row of any `prices_*.csv`, all text, + `source_file` | table (`ingest`) |
| `stg_orders` | staging | order; `order_date` = Riyadh day (UTC+3) | view |
| `stg_order_items` | staging | order line `(order_id, line_no)`; `line_revenue` | view |
| `stg_products`, `stg_stores` | staging | product (trimmed SKU) / store | view |
| `stg_supplier_prices` | staging | supplier file row; cost parsed, SKU trimmed | view |
| `int_supplier_quotes_checked` | intermediate | supplier file row + quarantine `reason` (NULL = usable) | view |
| `product_cost_history` | silver | SKU × cost version, valid on `[valid_from, valid_to)` | table |
| `price_quarantine` | quality | rejected supplier file row + reason | table |
| `fct_order_line_cost` | gold | counted order line, with the cost valid on its order date | table |
| `weekly_margin` | gold | iso_week × city × category with ≥ 1 counted line | table |

Cleaning rules live in macros (`dbt/macros/cleaning.sql`): `parse_cost` (decimal comma, Arabic-Indic digits and `٫` → a number, anything else → NULL) and `to_riyadh`.

**How the SCD2 history is built:** every supplier row is checked once in `int_supplier_quotes_checked` (unknown SKU first, then a cost that isn't a positive number); quarantined rows go nowhere else. For each `(sku, valid_from)` the row from the file whose name sorts last wins, so `prices_2026-W39_fix.csv` replaces `prices_2026-W39.csv`. Costs are rounded to 2 decimals and a row becomes a version only when its cost differs from the SKU's previous quote (`lag`); `valid_to` is the next version's `valid_from` (`lead`). A week that doesn't quote a SKU has no row for it, so the previous version simply stays open.

**How each order line finds its cost:** `fct_order_line_cost` left-joins each counted line to `product_cost_history` on the trimmed SKU and `valid_from <= order_date < valid_to` (open-ended when `valid_to` is NULL), so at most one version matches. Lines with no version stay in the fact with `is_costed = false`: they count in `lines` and `revenue_sar` only.

**Publishing:** each table is created once, then on every run compared with the new rows. If they're identical nothing is committed. If they differ, the old data files are replaced by the new ones in **one** Iceberg `overwrite` snapshot. (`table.overwrite()` would commit a delete and then an append, leaving an empty snapshot in between.)

## Data problems I found

Counts are for the course dataset (seed 2026) after the correction arrived.

| Problem | Where | Rows affected | What I did |
|---|---|---|---|
| Decimal commas (`9,49`) | `prices_2026-W38.csv` | 126 | `parse_cost`: `,` → `.` |
| Arabic-Indic digits (`٨٫٦٥`) | `prices_2026-W38.csv` | 34 | `parse_cost`: `translate` to ASCII digits and `.` |
| Byte-order mark | `prices_2026-W38.csv` | 1 file | DuckDB's CSV reader strips it; `supplier_id` header checked |
| `unit_cost` renamed to `cost_sar`, `currency` added, columns reordered | `prices_2026-W39*.csv` | 160 + 9 | `read_csv(union_by_name)`; cost = `coalesce(cost_sar, unit_cost)` |
| Zero or negative cost | `prices_2026-W39.csv` | 4 | quarantined as `invalid_cost` |
| SKUs Qafila doesn't sell (`QF-NEW-*`) | `prices_2026-W39.csv` | 3 | quarantined as `unknown_sku` |
| SKUs with trailing spaces | `products` | 2 | trimmed on both sides (supplier files had none in this seed) |
| Late correction | `prices_2026-W39_fix.csv` | 9 (4 fixes + 5 new prices) | latest file name wins per `(sku, valid_from)` |
| Zero or negative quantities | `order_items` (delivered, from 2026-08-31) | 69 | excluded from the margin |
| UTC date ≠ Riyadh date | `orders` | 712 | `order_date` = `created_at` + 3 h |
| Products never quoted / quoted only later | margin | 20,626 of 82,404 lines (190 SKUs) | kept as `uncosted_lines`, not in cost columns |

## Tests

`dbt build` runs 31 data tests and 2 unit tests; all pass.

| Test | On | What it protects against |
|---|---|---|
| `test_supplier_costs_are_parsed` (unit) | `stg_supplier_prices` | `9,49`, `٩٫٤٩`, `cost_sar`, negative and junk costs parsed wrongly |
| `test_scd2_versions_only_on_change_and_latest_file_wins` (unit) | `product_cost_history` | a version for an unchanged cost, a missing week closing a version, the original file beating `_fix` |
| `assert_one_current_version_per_sku` | `product_cost_history` | 0 or 2 open versions per SKU |
| `assert_cost_versions_are_contiguous` | `product_cost_history` | overlaps, gaps, empty intervals, non-positive or repeated costs |
| `unique` on `sku \|\| valid_from`, `relationships` sku → `stg_products` | `product_cost_history` | duplicate versions, costs for SKUs we don't sell |
| `accepted_values` on `reason` | `price_quarantine` | reasons outside the contract |
| `assert_weekly_margin_reconciles_to_source` | `weekly_margin` | lines or revenue lost or duplicated by a join |
| `assert_weekly_margin_adds_up` | `weekly_margin` | `margin ≠ costed revenue − cost`, costed revenue > revenue |
| `unique` on the grain | `weekly_margin`, `fct_order_line_cost`, `stg_order_items` | fan-out: a line costed twice, a duplicated group |
| `unique` / `not_null` on keys, `unique` trimmed SKU | staging | duplicate orders, products whose SKUs collide after trimming |

## Re-runs and the correction

Run on an empty `warehouse/`, then again on the same data, then after `make_data.py --add-correction`:

| Run | `gold.weekly_margin` snapshot | Snapshots | Total `margin_sar` | `product_cost_history` rows |
|---|---|---|---|---|
| 1 · empty warehouse | `1587502203690248303` (append) | 1 | 801,016.88 | 633 |
| 2 · same data | unchanged: "no new snapshot" | 1 | 801,016.88 | 633 |
| 3 · correction arrived | `3302395908977406120` (overwrite) | 2 | 803,515.80 | 637 |
| 4 · same data | unchanged | 2 | 803,515.80 | 637 |

A content hash of all three tables, and every snapshot id, was identical before and after run 2; run 4 committed nothing. Time travel after run 3:

```python
t = get_catalog(Path("warehouse")).load_table("gold.weekly_margin")
t.scan(snapshot_id=1587502203690248303).to_arrow()   # total margin 801,016.88, W39 248,962.65 (before the fix)
t.scan().to_arrow()                                  # total margin 803,515.80, W39 250,765.08
```

The quarantine didn't change with the correction: the 4 bad rows are still rejected rows of `prices_2026-W39.csv`; the fix file adds valid rows that take their place in the history.

I also ran the same sequence on a second dataset (`make_data.py --seed 11`): `check.py` passed 22/22 before and after the correction, and an independent Polars re-implementation of the rules, reading the raw files directly, matched all three tables exactly.

## What I'd do next

- **100× more data:** `fct_order_line_cost` and `weekly_margin` are rebuilt in full on every run. I'd make the fact incremental (as in Lab 2.3) with a look-back that covers the oldest supplier week a correction can touch, and partition the Iceberg tables by `iso_week` so a correction rewrites only the affected weeks instead of the whole table.
- **Daily supplier files:** the history logic doesn't assume weekly files, but the ingest re-reads every file each run. I'd load only new files (track loaded file names, as dlt does in Lab 1.3), validate them at the door with a schema contract, and alert the supplier on quarantined rows.
- **Iceberg upkeep:** expire old snapshots on a schedule (keeping enough for audits), and remove orphan files.

## AI assistance

I used Claude (Claude Code) as a pair programmer throughout. It helped profile the supplier files, write the DuckDB ingest, the dbt models, macros and tests, and the Iceberg publish step. It also wrote a throwaway Polars script (not committed) that re-implements the rules independently to cross-check the results on two seeds. I reviewed each step, ran the pipeline and checks myself, and made the design decisions recorded above.
