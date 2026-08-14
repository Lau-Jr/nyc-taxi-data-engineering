-- Phase 2: schema initialization for the NYC Yellow Taxi star schema.
-- Target engine: DuckDB (file-based, no server required for a class lab).
-- Run order: 01_create_schema.sql -> 02_create_dimensions.sql -> 03_create_fact.sql -> 04_analytical_queries.sql

DROP TABLE IF EXISTS fact_trip;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_vendor;
DROP TABLE IF EXISTS dim_payment;
DROP TABLE IF EXISTS dim_rate_code;
