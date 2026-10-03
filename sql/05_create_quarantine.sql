-- Phase 6: quarantine table for rows that fail a hard-reject quality check
-- (src/validation.py::HARD_REJECT_CHECKS). Failing rows are kept here with their raw
-- columns and a reason code instead of being silently dropped.
--
-- Unlike 01-03, this script uses CREATE TABLE IF NOT EXISTS and is run on every ingest by
-- src/database.py::ensure_schema, so an existing taxi.duckdb picks the table up without a
-- rebuild. Loads use INSERT ... ON CONFLICT (trip_id) DO NOTHING, so replaying a month
-- never quarantines the same row twice.
CREATE TABLE IF NOT EXISTS quarantine_trip (
    -- Raw TLC columns, as read from the source parquet (no renames, no dim lookups:
    -- the row failed validation, so nothing downstream of it is trusted).
    VendorID BIGINT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    RatecodeID DOUBLE,
    store_and_fwd_flag VARCHAR,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE,

    -- Same deterministic business key as fact_trip.trip_id (src/transform.py::add_trip_id).
    trip_id VARCHAR(64) NOT NULL,
    -- Comma-separated reason codes, e.g. 'unknown_vendor,pickup_outside_file_month'.
    reason_codes VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    ingest_month VARCHAR(7) NOT NULL,
    quarantined_at TIMESTAMP NOT NULL DEFAULT current_timestamp,

    CONSTRAINT uq_quarantine_trip_trip_id UNIQUE (trip_id)
);
