CREATE SEQUENCE IF NOT EXISTS seq_fact_trip_key START 1;

CREATE TABLE fact_trip (
    trip_key BIGINT PRIMARY KEY DEFAULT nextval('seq_fact_trip_key'),

    -- Deterministic business key (SHA-256 of VendorID, pickup/dropoff timestamps,
    -- PU/DO location, passenger_count, trip_distance, fare_amount, total_amount).
    -- The raw TLC file has no native trip identifier; this is a project-defined key
    -- used to make ingestion idempotent (see src/transform.py, docs/phase3).
    trip_id VARCHAR(64) NOT NULL,

    date_key INTEGER NOT NULL,

    pickup_location_key INTEGER,
    dropoff_location_key INTEGER,

    vendor_key INTEGER,
    payment_type_key INTEGER,
    rate_code_key INTEGER,

    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,

    passenger_count INTEGER,
    trip_distance NUMERIC(10,2),

    fare_amount NUMERIC(10,2),
    extra NUMERIC(10,2),
    mta_tax NUMERIC(10,2),
    tip_amount NUMERIC(10,2),
    tolls_amount NUMERIC(10,2),
    improvement_surcharge NUMERIC(10,2),
    congestion_surcharge NUMERIC(10,2),
    airport_fee NUMERIC(10,2),
    cbd_congestion_fee NUMERIC(10,2),
    total_amount NUMERIC(10,2),

    -- Soft-anomaly flag set by src/validation.py (e.g. zero passengers/distance,
    -- negative amounts). These rows are loaded, not discarded — see Problem B in
    -- docs/phase1/data_problem_statement.md.
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,

    -- UNIQUE stays: it is what makes re-running a month a no-op (docs/phase3).
    CONSTRAINT uq_fact_trip_trip_id UNIQUE (trip_id)

    -- Phase 9: the six FOREIGN KEY constraints (date_key -> dim_date, pickup/dropoff
    -- location -> dim_location, vendor/payment/rate_code -> their dims) were removed. DuckDB
    -- checked them with 6 index lookups per inserted row, which made the fact insert ~2x
    -- slower (docs/phase9/performance_report.md). The same rule is now enforced once per
    -- batch by src/database.py::check_fact_foreign_keys, which refuses the whole batch
    -- before anything is written if any key has no matching dimension row.
);
