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

    CONSTRAINT uq_fact_trip_trip_id UNIQUE (trip_id),
    CONSTRAINT fk_fact_trip_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_trip_pickup_loc FOREIGN KEY (pickup_location_key) REFERENCES dim_location (location_key),
    CONSTRAINT fk_fact_trip_dropoff_loc FOREIGN KEY (dropoff_location_key) REFERENCES dim_location (location_key),
    CONSTRAINT fk_fact_trip_vendor FOREIGN KEY (vendor_key) REFERENCES dim_vendor (vendor_key),
    CONSTRAINT fk_fact_trip_payment FOREIGN KEY (payment_type_key) REFERENCES dim_payment (payment_type_key),
    CONSTRAINT fk_fact_trip_rate FOREIGN KEY (rate_code_key) REFERENCES dim_rate_code (rate_code_key)
);
