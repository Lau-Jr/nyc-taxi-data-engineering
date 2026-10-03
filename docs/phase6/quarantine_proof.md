# Phase 6 — Proof of Quarantine

Rows that fail a hard quality check (`src/validation.py::HARD_REJECT_CHECKS`) are not
dropped. They go to the `quarantine_trip` table (`sql/05_create_quarantine.sql`) with their
raw columns and a reason code, and they never reach `fact_trip`. This document shows that
with (1) one deliberately bad row and (2) a full replay of the January 2026 file.

## 1. One deliberately bad row

`tests/test_quality.py::test_bad_row_is_quarantined_once_and_never_loaded` writes a
three-row parquet in which row 2 is deliberately broken: `VendorID = 99` (not in
`dim_vendor`) and a pickup on **2030-01-01** (in the future, and outside the `2026-01` file
month). It then runs `ingest.run("2026-01", ...)` twice against a fresh database.

### Run 1

```
INFO Rows read: 3
INFO Rows valid: 2
INFO Rows rejected: 1
WARNING Quarantined [validity] pickup_in_future: 1 rows
WARNING Quarantined [validity] pickup_outside_file_month: 1 rows
WARNING Quarantined [validity] unknown_vendor: 1 rows
INFO Rows quarantined (new this run): 1
INFO quarantine_trip row count after run: 1
INFO Rows inserted: 2
INFO fact_trip row count after run: 2
```

### Run 2 (same file, replayed)

```
INFO Rows rejected: 1
WARNING Quarantined [validity] pickup_in_future: 1 rows
WARNING Quarantined [validity] pickup_outside_file_month: 1 rows
WARNING Quarantined [validity] unknown_vendor: 1 rows
INFO Rows quarantined (new this run): 0
INFO quarantine_trip row count after run: 1
INFO Rows inserted: 0
INFO Rows skipped (already present): 2
INFO fact_trip row count after run: 2
```

### What is in the database afterwards

```
SELECT trip_id, VendorID, tpep_pickup_datetime, reason_codes, source_file, ingest_month
FROM quarantine_trip;

     trip_id  VendorID tpep_pickup_datetime                                              reason_codes                     source_file ingest_month
730370eed400        99  2030-01-01 09:00:00 pickup_in_future,pickup_outside_file_month,unknown_vendor yellow_tripdata_2026-01.parquet      2026-01
```
(`trip_id` truncated here; it is the full SHA-256 in the table.)

```
SELECT COUNT(*) FROM fact_trip f JOIN quarantine_trip q USING (trip_id);   -- 0
```

The test asserts each of these facts:

| Claim | Assertion |
|---|---|
| The bad row is quarantined | `quarantine_trip` holds exactly that `trip_id`, with `VendorID = 99` |
| It carries the right reason codes | `reason_codes = 'pickup_in_future,pickup_outside_file_month,unknown_vendor'` |
| It never reaches `fact_trip` | `SELECT COUNT(*) FROM fact_trip WHERE trip_id = <bad id>` → 0 |
| It is quarantined only once | run 2 `rows_quarantined == 0`, `quarantine_trip_count == 1` |
| The clean rows still load | run 1 `rows_inserted == 2` |
| The rejected-rows log agrees | `logs/rejected_2026-01.csv` lists exactly the bad `trip_id` |

Each new check also has its own unit test in `tests/test_quality.py`:

```
python -m pytest tests/ -v      # 21 passed (5 Phase 3 tests + 16 Phase 6 tests)
```

## 2. Full January 2026 file

`python -m src.ingest --month 2026-01`, run twice against a fresh database:

| | Run 1 | Run 2 (replay) |
|---|---:|---:|
| Rows read | 3,724,889 | 3,724,889 |
| Rows rejected (hard checks) | 45,077 | 45,077 |
| Rows quarantined (new) | 45,077 | **0** |
| `quarantine_trip` count after run | 45,077 | 45,077 |
| Rows inserted into `fact_trip` | 3,679,812 | **0** |
| `fact_trip` count after run | 3,679,812 | 3,679,812 |

```
SELECT reason_codes, count(*) FROM quarantine_trip GROUP BY 1 ORDER BY 2 DESC;

reason_codes                     count
dropoff_before_or_equal_pickup   45,070
pickup_outside_file_month             7
```

```
SELECT COUNT(*) FROM fact_trip JOIN quarantine_trip USING (trip_id);   -- 0
SELECT COUNT(*) - COUNT(DISTINCT trip_id) FROM quarantine_trip;         -- 0
```

The 7 rows outside the file month are new catches. Six were picked up on 2025-12-31 and one
on 2026-02-01. Two of the December rows are a +$71.50 / −$71.50 pair (a charge and its
reversal). Before Phase 6 these 7 rows passed validation and loaded into `fact_trip`, which
is why that table now has 3,679,812 rows instead of the 3,679,819 in the Phase 3 proof. No
row hit any other hard check in this file (no future pickups, unknown codes, out-of-range
locations or in-file duplicates), but the checks run on every month.

The batch completeness log for the same run:

```
WARNING Completeness: passenger_count is 29.21% null (threshold 5%)
WARNING Completeness: RatecodeID is 29.21% null (threshold 5%)
WARNING Completeness: store_and_fwd_flag is 29.21% null (threshold 5%)
WARNING Completeness: congestion_surcharge is 29.21% null (threshold 5%)
WARNING Completeness: Airport_fee is 29.21% null (threshold 5%)
```

These five columns are null on the same 1,088,058 rows. Those rows are loaded (with
`passenger_count` null they are flagged soft anomalies), not quarantined.

The soft checks flagged 1,171,219 rows; these are loaded with `is_anomaly = TRUE`. That
includes the new `trip_over_24h` check, which flagged 35 rows.

## Why replay doesn't duplicate quarantine rows

`quarantine_trip` keys on the same deterministic `trip_id` as `fact_trip`
(`src/transform.py::add_trip_id`). It has `UNIQUE (trip_id)` and is loaded with
`INSERT ... ON CONFLICT (trip_id) DO NOTHING` (`src/database.py::insert_quarantine_rows`),
which is the same idempotency mechanism as the fact load in Phase 3.

## Existing databases

`sql/05_create_quarantine.sql` is `CREATE TABLE IF NOT EXISTS`, and
`database.ensure_schema` runs it on every ingest, outside the "fact_trip already exists"
early exit. A database built in Phase 3 gains the table on its next run without a rebuild
(`test_existing_database_gains_quarantine_table_without_rebuild`). However, a Phase 3
database still has the 7 out-of-month January rows in `fact_trip`, because they were loaded
before the check existed. To bring `fact_trip` in line, rebuild with `make clean` followed
by `make ingest`.
