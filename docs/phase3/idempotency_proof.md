# Phase 3 — Proof of Idempotency

Command run twice, unmodified, against a freshly created database:

```
python -m src.ingest --month 2026-01
```

## Run 1 (fresh database)

```
Rows read:       3,724,889
Rows valid:      3,679,819
Rows rejected:      45,070
Rows inserted:   3,679,819
Rows skipped:            0

fact_trip count after run: 3,679,819
```

## Run 2 (same command, same file, no changes)

```
Rows read:       3,724,889
Rows valid:      3,679,819
Rows rejected:      45,070
Rows inserted:            0
Rows skipped:    3,679,819

fact_trip count after run: 3,679,819
```

## Result

| | Before run 2 | After run 2 |
|---|---:|---:|
| `fact_trip` row count | 3,679,819 | 3,679,819 |

The row count is unchanged. Every row that made it through validation on the second run
already had a matching `trip_id` in the table, so all 3,679,819 inserts were skipped by
`INSERT ... ON CONFLICT (trip_id) DO NOTHING` (`src/database.py::insert_fact_trips`).

## Why this works

`trip_id` is not supplied by the source file — TLC's raw parquet has no trip identifier.
`src/transform.py::add_trip_id` derives one deterministically as
`SHA-256(VendorID | pickup_datetime | dropoff_datetime | PULocationID | DOLocationID |
passenger_count | trip_distance | fare_amount | total_amount)`. Because the input fields are
unchanged between runs, the hash is unchanged, so the second run produces exactly the same
3,679,819 `trip_id` values as the first — and the database's `UNIQUE (trip_id)` constraint
(`sql/03_create_fact.sql`) rejects every one of them on the second insert.

This was also checked directly against the database:

```
SELECT COUNT(*) - COUNT(DISTINCT trip_id) FROM fact_trip;  -- 0
```

## What "re-runnable" does *not* wipe

`src/database.py::ensure_schema` only executes `sql/01_create_schema.sql` (which contains
`DROP TABLE IF EXISTS ...`) the first time it sees no `fact_trip` table in the target
database. On every subsequent run it is skipped, so re-running the ingester never drops or
recreates tables that already hold data — the idempotency guarantee lives entirely in the
`trip_id` uniqueness constraint, not in "start from a clean slate every time".

## Rejected rows are not silently discarded

The 45,070 rejected rows (`dropoff_datetime <= pickup_datetime` — the only hard-reject
condition triggered in this file; `PULocationID`/`DOLocationID`/pickup/dropoff timestamps are
never missing in the January file) are written with a reason code to
`logs/rejected_2026-01.csv`, and per-run summary counts are appended to
`logs/ingestion_2026-01.log`.
