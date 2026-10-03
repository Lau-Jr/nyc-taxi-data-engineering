-- Phase 2: schema initialization for the NYC Yellow Taxi star schema.
-- Target engine: DuckDB (file-based, no server required for a class lab).
-- Run order: 01_create_schema.sql -> 02_create_dimensions.sql -> 03_create_fact.sql
--            -> 05_create_quarantine.sql -> 06_create_pipeline_run.sql
--            -> 07_refresh_mart_daily_zone_trips.sql -> 04_analytical_queries.sql

DROP TABLE IF EXISTS mart_daily_zone_trips;
DROP TABLE IF EXISTS pipeline_run;
DROP SEQUENCE IF EXISTS seq_pipeline_run_id;
DROP TABLE IF EXISTS quarantine_trip;
DROP TABLE IF EXISTS fact_trip;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_vendor;
DROP TABLE IF EXISTS dim_payment;
DROP TABLE IF EXISTS dim_rate_code;
