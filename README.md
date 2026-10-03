# NYC Taxi Data Engineering

A re-runnable data pipeline for NYC TLC Yellow Taxi trip records: raw-data profiling, a
DuckDB star schema, and an idempotent Python ingester.

- Phase 1 — [Data Problem Statement](docs/phase1/data_problem_statement.md)
- Phase 2 — [Schema design](docs/phase2/schema_design.md), [data dictionary](docs/phase2/data_dictionary.md), [SQL](sql/)
- Phase 3 — [Idempotency proof](docs/phase3/idempotency_proof.md)
- Phase 5 — [Cloud pipeline design](docs/phase5/cloud_pipeline_design.md)
- Phase 6 — [Quarantine proof](docs/phase6/quarantine_proof.md), [quality checks](#data-quality-handling), [data lineage](#data-lineage), [PDPA assessment](#pdpa-tanzania-assessment)

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
validates and loads `data/raw/yellow_tripdata_2026-01.parquet`, quarantines failing rows in
the `quarantine_trip` table, and writes a summary to `logs/ingestion_2026-01.log` plus
rejected rows to `logs/rejected_2026-01.csv`. Running the
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

Every check is executable code in `src/validation.py`, runs on every ingest, and is tagged
with the quality dimension it guards (`CHECK_DIMENSIONS`). Validation splits rows into two
tiers:

- **Hard reject → quarantine.** The record can't be analytically valid. It is excluded from
  `fact_trip`, inserted into the **`quarantine_trip`** table (`sql/05_create_quarantine.sql`)
  with all of its raw columns, its `trip_id`, a comma-separated `reason_codes`, the
  `source_file`, `ingest_month` and `quarantined_at`, and also listed in
  `logs/rejected_<month>.csv`. The ingest log has a WARNING line per reason code with its
  row count. Quarantine loads use `ON CONFLICT (trip_id) DO NOTHING`, so replaying a month
  never quarantines the same row twice.
- **Soft anomaly → load and flag.** The record is implausible but not unusable. It is
  loaded with `fact_trip.is_anomaly = TRUE`, so downstream queries can include or exclude
  it explicitly (Problem B in the Phase 1 statement).

| Reason code | Dimension | Tier | Rule | Jan 2026 rows |
|---|---|---|---|---:|
| `missing_pickup_datetime` / `missing_dropoff_datetime` | completeness | hard | timestamp is null | 0 |
| `missing_pickup_location` / `missing_dropoff_location` | completeness | hard | `PULocationID` / `DOLocationID` is null | 0 |
| `dropoff_before_or_equal_pickup` | validity | hard | `dropoff <= pickup` | 45,070 |
| `pickup_in_future` | validity | hard | pickup later than the time of the run | 0 |
| `pickup_outside_file_month` | validity | hard | pickup not inside the `--month` being ingested | 7 |
| `unknown_vendor` | validity | hard | `VendorID` not in seeded `dim_vendor` | 0 |
| `unknown_payment_type` | validity | hard | `payment_type` not in seeded `dim_payment` | 0 |
| `unknown_rate_code` | validity | hard | `RatecodeID` present but not in seeded `dim_rate_code` (null is allowed) | 0 |
| `location_id_out_of_range` | validity | hard | PU/DO location outside the TLC zone range 1–265 | 0 |
| `duplicate_in_batch` | uniqueness | hard | `trip_id` already seen earlier in the same file (first copy kept) | 0 |
| `zero_or_missing_passenger_count` | validity | soft | `passenger_count` ≤ 0 or null | 1,102,689 |
| `zero_or_negative_trip_distance` | validity | soft | `trip_distance` ≤ 0 | 124,574 |
| `negative_fare_amount` / `negative_total_amount` | validity | soft | amount < 0 | 39,462 / 39,983 |
| `trip_over_24h` | validity | soft | dropoff more than 24 h after pickup | 35 |

The vendor, payment and rate-code checks read their valid codes from the seeded dimension
tables at run time, so `sql/02_create_dimensions.sql` is the single source of truth for what
counts as a known code. Uniqueness is also enforced across runs by `UNIQUE (trip_id)` on
`fact_trip` and `quarantine_trip`.

**Batch completeness.** Before the row checks run, `validation.log_completeness` logs the
null rate of every column and raises a WARNING above 5%. In January, `passenger_count`,
`RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge` and `Airport_fee` are each
29.21% null (always the same rows). Those fields are optional, so the rows are loaded rather
than rejected, but a change in that rate from one month to the next shows up in the log.

Proof that one deliberately bad row is quarantined with the right reason code, never reaches
`fact_trip`, and is quarantined only once on replay:
[docs/phase6/quarantine_proof.md](docs/phase6/quarantine_proof.md) (`tests/test_quality.py`).

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

## Data lineage

Every table and every hop, from the TLC download to a query result:

```
TLC trip-record download (nyc.gov, monthly parquet)
   │
   ▼
data/raw/yellow_tripdata_YYYY-MM.parquet                       (raw, immutable, 1 row = 1 trip)
   │  src/ingest.py  →  src/transform.py::add_trip_id         (SHA-256 business key)
   │                 →  src/validation.py                      (completeness log + hard/soft checks)
   │
   ├── hard reject ──► quarantine_trip          (data/processed/taxi.duckdb)
   │                   logs/rejected_YYYY-MM.csv  (trip_id + reason)
   │                   logs/ingestion_YYYY-MM.log (counts per reason code)
   │
   └── pass ──► src/transform.py  (date_key, dim key lookups, is_anomaly flag)
                  │
                  ├──► dim_date       (built from pickup dates in the batch)
                  ├──► dim_location   (built from PU/DO ids in the batch)
                  │    dim_vendor / dim_payment / dim_rate_code  (seeded by sql/02, read here)
                  ▼
                fact_trip  (data/processed/taxi.duckdb)
                  │
                  ▼
                sql/04_analytical_queries.sql  →  query results

Side branch (Phase 5):
data/raw/yellow_tripdata_2026-01.parquet
   │  100,000-row extract, exported to CSV (unvalidated raw rows)
   ▼
data/samples/yellow_tripdata_2026-01_sample100k.csv
   │  manual upload, BigQuery console
   ▼
BigQuery leaning-de-and-sys-admin.NYC_YELLOW_TAX.trips_sample   (region africa-south1)
   │
   ▼
Phase 5 top-10-by-revenue query  (docs/phase5/cloud_pipeline_design.md)
```

| Table / file | Source | Written by | Grain |
|---|---|---|---|
| `data/raw/yellow_tripdata_YYYY-MM.parquet` | NYC TLC trip-record download | manual download, never modified | one row per trip as reported by the vendor |
| `quarantine_trip` | raw parquet rows that fail a hard check | `src/ingest.py` → `database.insert_quarantine_rows` (DDL `sql/05`) | one row per rejected `trip_id` |
| `logs/rejected_YYYY-MM.csv` | same rejected rows | `src/ingest.py::write_rejected_rows` (overwritten each run) | one row per rejected raw row: `trip_id`, `reason` |
| `logs/ingestion_YYYY-MM.log` | run counts, completeness, reason counts | `src/ingest.py` logging (appended each run) | one block per run |
| `dim_date` | distinct pickup dates of rows that passed | `transform.build_dim_date_rows` → `database.upsert_dim_date` | one row per calendar day |
| `dim_location` | distinct PU/DO ids of rows that passed | `transform.build_dim_location_rows` → `database.upsert_dim_location` | one row per TLC location id (no zone names: no lookup CSV) |
| `dim_vendor` | TLC data dictionary | seeded by `sql/02_create_dimensions.sql` | one row per vendor code |
| `dim_payment` | TLC data dictionary + observed code 0 | seeded by `sql/02_create_dimensions.sql` | one row per payment code |
| `dim_rate_code` | TLC data dictionary + observed code 99 | seeded by `sql/02_create_dimensions.sql` | one row per rate code |
| `fact_trip` | raw rows that passed every hard check | `transform.prepare_fact_rows` → `database.insert_fact_trips` | one row per unique `trip_id` |
| `data/samples/yellow_tripdata_2026-01_sample100k.csv` | 100,000 rows of the January raw parquet | one-off extract (the script is not in this repo) | one row per trip, raw columns, no validation |
| BigQuery `trips_sample` | the sample CSV | manual console load | same as the sample CSV |

**Tracing the Phase 5 figure.** The Phase 5 result (*PULocationID 132, 4,117 trips,
$290,902.56*) traces back like this:

1. **BigQuery `trips_sample`.** `SELECT PULocationID, COUNT(*), SUM(total_amount) ... GROUP BY PULocationID`.
2. **`data/samples/yellow_tripdata_2026-01_sample100k.csv`.** The same aggregate run locally
   on the CSV gives exactly 4,117 trips and $290,902.56 for location 132, so the load into
   BigQuery lost nothing.
3. **`data/raw/yellow_tripdata_2026-01.parquet`.** All 100,000 sample rows match a raw row
   on vendor, both timestamps, both locations and `total_amount`. They are spread across
   the whole month (01-01 00:00:38 to 01-31 23:59:33), so the sample is a random extract,
   not the first 100k rows. The full month has 152,589 trips and $11,063,083.51 for
   location 132. Of those, 151,353 trips and $10,959,150.46 reach `fact_trip` (this count
   still includes soft anomalies).

The sample was taken *before* validation. 1,215 of its 100,000 rows (32 of them from
location 132) have `dropoff <= pickup`, so the local pipeline would quarantine them. The
BigQuery figure therefore includes rows that never reach `fact_trip`. A production cloud
load should read from validated output, not from the raw file (boxes 4–6 of the Phase 5
design).

## PDPA (Tanzania) assessment

This walks through the four steps of the Personal Data Protection Act, 2022 assessment.
(1) *Is it personal data?* The TLC files carry no direct identifiers: no names, plates,
medallion or licence numbers, and no card numbers. However, a pickup timestamp plus zone
and a dropoff timestamp plus zone together act as a quasi-identifier. In the 2014 NYC taxi
release, poorly hashed medallion numbers were reversed, and trips were then linked to
named people photographed getting into cabs. Anyone who knows when and where someone took
a taxi could in principle find that trip here, so we treat row-level trips as potentially
personal data. (2) *Purpose and lawful basis.* The purpose is coursework analytics on data
that NYC TLC publishes openly for public reuse. The data subjects are passengers in the
United States, and nothing is collected from people in Tanzania, so the Act's obligations
apply only in a limited way. We still apply its principles: purpose limitation, minimisation,
security and storage limitation. (3) *Minimisation.* The star schema keeps only the columns
the analysis needs (timestamps, zone ids, distances, amounts and code keys). Nothing is
enriched with outside data that would make re-identification easier, and dimensions hold
codes, not people. (4) *Protection and retention.* Results are reported only as aggregates
at zone/day level or coarser, and no row-level extracts derived from the pipeline are
shared. `data/processed` (the DuckDB file, including `quarantine_trip`) and `logs` are
git-ignored. Only named course members have access to the local database and the BigQuery
project. The raw TLC parquet files and the 100k sample CSV *are* committed to this
repository. They are unmodified copies of files TLC already publishes, so committing them
adds no new exposure, but it does mean the repository holds row-level trips. Before the
repository is shared outside the course, they should be removed from history and replaced
with a download step. The
Phase 5 upload of 100,000 raw rows to BigQuery in `africa-south1` (Johannesburg) is a
**cross-border transfer** out of Tanzania, which the Act regulates. It was kept to a
sample, within one named project. The raw files, the DuckDB database, the logs, the sample
CSV and the BigQuery dataset will be deleted at the end of the semester.
