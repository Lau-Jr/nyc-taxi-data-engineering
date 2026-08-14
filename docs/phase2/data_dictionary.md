# Data Dictionary

Column-by-column mapping from the raw TLC parquet fields to the DuckDB star schema defined
in `sql/01_create_schema.sql`–`sql/03_create_fact.sql`. "Raw source" is the column name in
`yellow_tripdata_2026-01.parquet`; "—" means the column has no raw counterpart (it is
derived at ingest time by `src/transform.py`).

## fact_trip

| Column | Type | Raw source | Description |
|---|---|---|---|
| `trip_key` | BIGINT PK | — | Surrogate key, sequence-generated on insert. |
| `trip_id` | VARCHAR(64) UNIQUE | — | Deterministic SHA-256 of `VendorID`, pickup/dropoff timestamps, `PULocationID`, `DOLocationID`, `passenger_count`, `trip_distance`, `fare_amount`, `total_amount` (`src/transform.py::add_trip_id`). Not a TLC identifier — the raw file has none. This is what makes the loader idempotent (`sql/03_create_fact.sql`'s `UNIQUE` constraint + `ON CONFLICT DO NOTHING`). |
| `date_key` | INTEGER FK → `dim_date` | — | `YYYYMMDD` integer derived from `tpep_pickup_datetime`'s date. |
| `pickup_location_key` | INTEGER FK → `dim_location` | `PULocationID` | Copied as-is; `dim_location.location_key = location_id`, so no lookup step is needed. |
| `dropoff_location_key` | INTEGER FK → `dim_location` | `DOLocationID` | Same as above. |
| `vendor_key` | INTEGER FK → `dim_vendor` | `VendorID` | Mapped through `dim_vendor.vendor_id`. |
| `payment_type_key` | INTEGER FK → `dim_payment` | `payment_type` | Mapped through `dim_payment.payment_type_id`, including the unofficial code `0` ("Unknown / not provided" — see Problem C in the Phase 1 statement). |
| `rate_code_key` | INTEGER FK → `dim_rate_code`, nullable | `RatecodeID` | Mapped through `dim_rate_code.rate_code_id`, including the unofficial code `99`. **NULL** when `RatecodeID` itself is missing in the raw row (the 29.21% missingness cluster in Problem A) — deliberately left unmapped rather than forced to the "Unknown" code, since "not provided" and "explicitly coded 99" are different data-quality signals. |
| `pickup_datetime` | TIMESTAMP NOT NULL | `tpep_pickup_datetime` | Renamed on load. |
| `dropoff_datetime` | TIMESTAMP NOT NULL | `tpep_dropoff_datetime` | Renamed on load. |
| `passenger_count` | INTEGER, nullable | `passenger_count` | NULL for the same 29.21% missingness cluster. |
| `trip_distance` | NUMERIC(10,2) | `trip_distance` | Miles, as reported. |
| `fare_amount` | NUMERIC(10,2) | `fare_amount` | Can be negative (refund/reversal — see Problem B). |
| `extra` | NUMERIC(10,2) | `extra` | Miscellaneous surcharges (rush hour, overnight). |
| `mta_tax` | NUMERIC(10,2) | `mta_tax` | Fixed MTA tax. |
| `tip_amount` | NUMERIC(10,2) | `tip_amount` | Credit-card tips only; cash tips are not captured by TLC. |
| `tolls_amount` | NUMERIC(10,2) | `tolls_amount` | Tolls charged during the trip. |
| `improvement_surcharge` | NUMERIC(10,2) | `improvement_surcharge` | Fixed TLC surcharge. |
| `congestion_surcharge` | NUMERIC(10,2) | `congestion_surcharge` | NYC congestion pricing surcharge; null for the missingness cluster. |
| `airport_fee` | NUMERIC(10,2) | `Airport_fee` | Renamed to lowercase on load; null for the missingness cluster. |
| `cbd_congestion_fee` | NUMERIC(10,2) | `cbd_congestion_fee` | Central Business District congestion fee. |
| `total_amount` | NUMERIC(10,2) | `total_amount` | Sum of the above; can be negative (refund/reversal). |
| `is_anomaly` | BOOLEAN NOT NULL | — | `TRUE` if the row trips a soft-anomaly check (`src/validation.py::SOFT_ANOMALY_CHECKS`): zero/missing `passenger_count`, zero/negative `trip_distance`, negative `fare_amount`, or negative `total_amount`. Row is still loaded; queries filter on this explicitly instead of the pipeline silently dropping it. |

## dim_date

Populated at ingest time from the distinct pickup dates in the batch being loaded
(`src/transform.py::build_dim_date_rows`), not seeded — its contents depend on which
month(s) have been ingested.

| Column | Type | Description |
|---|---|---|
| `date_key` | INTEGER PK | `YYYYMMDD`, same value as `fact_trip.date_key`. |
| `full_date` | DATE UNIQUE | Calendar date. |
| `day` / `month` / `quarter` / `year` | INTEGER | Calendar parts. |
| `day_of_week` | INTEGER | 0 = Monday … 6 = Sunday (pandas `.dt.dayofweek` convention). |
| `day_name` | VARCHAR(10) | e.g. `"Monday"`. |

## dim_location

Populated at ingest time from the distinct `PULocationID`/`DOLocationID` values actually
seen in the batch (`src/transform.py::build_dim_location_rows`). `borough`, `zone`, and
`service_zone` are left `NULL` — no TLC Taxi Zone Lookup CSV ships with this dataset drop, so
only the raw location id is known.

| Column | Type | Description |
|---|---|---|
| `location_key` | INTEGER PK | Equal to `location_id` (no synthetic surrogate needed — the id space is already small and stable, 1–265). |
| `location_id` | INTEGER UNIQUE | Raw `PULocationID`/`DOLocationID` value. |
| `borough` / `zone` / `service_zone` | VARCHAR, nullable | Not populated — see above. |

## dim_vendor, dim_payment, dim_rate_code

Small, low-cardinality code lookups. Seeded directly in `sql/02_create_dimensions.sql`
with hand-assigned surrogate keys (unlike `dim_location`, since these tables also need an
explicit row for codes that appear in the data but aren't in TLC's published dictionary).

| Table | Natural id column | Values seeded |
|---|---|---|
| `dim_vendor` | `vendor_id` | 1, 2, 6, 7 (all four `VendorID` values observed in the January file) |
| `dim_payment` | `payment_type_id` | 0–6, where 0 is the unofficial "Unknown / not provided" code |
| `dim_rate_code` | `rate_code_id` | 1–6, plus the unofficial 99 ("Unknown / not provided") |
