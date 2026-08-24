# NYC Taxi Data Engineering

A re-runnable data pipeline for NYC TLC Yellow Taxi trip records: raw-data profiling, a
DuckDB star schema, and an idempotent Python ingester.

- Phase 1 — [Data Problem Statement](docs/phase1/data_problem_statement.md)
- Phase 2 — [Schema design](docs/phase2/schema_design.md), [data dictionary](docs/phase2/data_dictionary.md), [SQL](sql/)
- Phase 3 — [Idempotency proof](docs/phase3/idempotency_proof.md)
- Phase 5 — [Cloud pipeline design](docs/phase5/cloud_pipeline_design.md)

## Run it

```
pip install -r requirements.txt
python -m src.ingest --month 2026-01
```

or, if `make` is available:

```
make ingest MONTH=2026-01
```

This creates `data/processed/taxi.duckdb`, applies the schema in `sql/` on first run,
validates and loads `data/raw/yellow_tripdata_2026-01.parquet`, and writes a summary to
`logs/ingestion_2026-01.log` plus rejected rows to `logs/rejected_2026-01.csv`. Running the
same command again is safe — it inserts 0 new rows the second time (see
[docs/phase3/idempotency_proof.md](docs/phase3/idempotency_proof.md)).

Run tests with `python -m pytest tests/ -v` or `make test`.

## Schema design

A star schema was selected because the primary workload is analytical rather than
transactional. Trip measurements such as distance, passenger count and monetary amounts
belong in the fact table, while descriptive attributes such as location, payment type,
vendor and date are represented as dimensions. This reduces repeated descriptive data and
makes aggregation queries straightforward — see `sql/04_analytical_queries.sql` for examples
that join two and three dimensions at once.

**Why not one big table?** Every trip in the raw file already repeats the same handful of
codes (`VendorID`, `payment_type`, `RatecodeID`) millions of times. Denormalizing those into
descriptive text on every row would multiply storage for no analytical benefit and make a
vendor-name typo fix touch every trip instead of one row. The fact table stores only the
numeric measurements and the foreign keys; descriptive text lives once per dimension.

`dim_vendor`, `dim_payment`, and `dim_rate_code` are small, low-cardinality code lookups —
they are seeded directly in `sql/02_create_dimensions.sql`, including an explicit "Unknown /
not provided" member for codes that appear in the raw data but aren't part of TLC's published
dictionary (`payment_type = 0`, `RatecodeID = 99`; see Problem C in the Phase 1 statement).
Seeding those members means a fact row carrying an unrecognized code still joins cleanly
instead of being silently dropped from every dimension query. `dim_date` and `dim_location`
are populated at ingest time instead, since they depend on what's actually in the trip file
(and no TLC Taxi Zone Lookup CSV ships with this dataset drop, so `dim_location` has no
borough/zone names — only the location id).

## Data quality handling

Validation splits rows into two tiers (`src/validation.py`):

- **Hard reject** — the record can't be analytically valid (missing pickup/dropoff
  timestamp or location, or `dropoff_datetime <= pickup_datetime`). These rows are excluded
  from the load and written to `logs/rejected_<month>.csv` with a reason code — never
  silently discarded.
- **Soft anomaly** — the record is implausible but not unusable (zero/missing passenger
  count, zero or negative trip distance, negative fare/total amount — see Problem B in the
  Phase 1 statement). These rows are loaded with `fact_trip.is_anomaly = TRUE` so downstream
  queries can include or exclude them explicitly.

## Idempotency

The raw file has no native trip identifier. `src/transform.py::add_trip_id` derives one
deterministically as a SHA-256 hash of `VendorID`, pickup/dropoff timestamps, PU/DO location,
passenger count, trip distance, fare amount, and total amount. `fact_trip.trip_id` is
`UNIQUE`, and loads use `INSERT ... ON CONFLICT (trip_id) DO NOTHING`, so re-running the
ingester on the same file is a no-op at the database level. Proof: see
[docs/phase3/idempotency_proof.md](docs/phase3/idempotency_proof.md).

## Cloud warehouse (Phase 5)

A 100,000-row sample (`data/samples/yellow_tripdata_2026-01_sample100k.csv`) was loaded into
a BigQuery sandbox table and queried with a top-10-by-revenue aggregate over `PULocationID`:
**1.53 MB processed, 10 MB billed** — the gap is BigQuery's per-query minimum billing
quantum, not extra data scanned. The full paper design for a production version of this
pipeline (every box, every arrow, a cost note on each — tied back to that bytes-processed
figure as the real cost driver) is in
[docs/phase5/cloud_pipeline_design.md](docs/phase5/cloud_pipeline_design.md).
