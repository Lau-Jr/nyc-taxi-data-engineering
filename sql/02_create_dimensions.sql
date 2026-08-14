-- dim_date and dim_location are populated at load time by src/ingest.py, not seeded here:
-- dim_date is generated for whichever month is being ingested, and dim_location is built
-- from the PULocationID/DOLocationID values actually observed in the trip file (no official
-- TLC Taxi Zone Lookup CSV is included in data/raw for this dataset drop).
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL
);

CREATE TABLE dim_payment (
    payment_type_key INTEGER PRIMARY KEY,
    payment_type_id INTEGER NOT NULL UNIQUE,
    payment_description VARCHAR(100)
);

CREATE TABLE dim_location (
    location_key INTEGER PRIMARY KEY,
    location_id INTEGER NOT NULL UNIQUE,
    borough VARCHAR(50),
    zone VARCHAR(100),
    service_zone VARCHAR(50)
);

CREATE TABLE dim_vendor (
    vendor_key INTEGER PRIMARY KEY,
    vendor_id INTEGER NOT NULL UNIQUE,
    vendor_name VARCHAR(100)
);

CREATE TABLE dim_rate_code (
    rate_code_key INTEGER PRIMARY KEY,
    rate_code_id INTEGER NOT NULL UNIQUE,
    rate_description VARCHAR(100)
);

-- Seed rows for low-cardinality dimensions, sourced from the TLC data dictionary.
-- Codes 0 (payment_type) and 99 (RatecodeID) are not part of the official dictionary but
-- appear in the raw file (see docs/phase1/data_problem_statement.md, Problem C) — they are
-- seeded explicitly as "Unknown" members so fact rows carrying them still join cleanly
-- instead of being silently dropped.
INSERT INTO dim_vendor (vendor_key, vendor_id, vendor_name) VALUES
    (1, 1, 'Creative Mobile Technologies, LLC'),
    (2, 2, 'Curb Mobility, LLC'),
    (3, 6, 'Myle Technologies Inc'),
    (4, 7, 'Helix');

INSERT INTO dim_payment (payment_type_key, payment_type_id, payment_description) VALUES
    (1, 0, 'Unknown / not provided'),
    (2, 1, 'Credit card'),
    (3, 2, 'Cash'),
    (4, 3, 'No charge'),
    (5, 4, 'Dispute'),
    (6, 5, 'Unknown'),
    (7, 6, 'Voided trip');

INSERT INTO dim_rate_code (rate_code_key, rate_code_id, rate_description) VALUES
    (1, 1, 'Standard rate'),
    (2, 2, 'JFK'),
    (3, 3, 'Newark'),
    (4, 4, 'Nassau or Westchester'),
    (5, 5, 'Negotiated fare'),
    (6, 6, 'Group ride'),
    (7, 99, 'Unknown / not provided');