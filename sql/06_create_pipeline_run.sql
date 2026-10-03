-- Phase 7: one row per ingest run, written by src/ingest.py::run. The dashboard's freshness
-- label is computed from this table and from mart_daily_zone_trips, never typed by hand
-- (src/serving.py::get_freshness).
--
-- Like 05, this script is CREATE ... IF NOT EXISTS and is run on every ingest by
-- src/database.py::ensure_schema, so an existing taxi.duckdb gains the table without a rebuild.
CREATE SEQUENCE IF NOT EXISTS seq_pipeline_run_id START 1;

CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id BIGINT PRIMARY KEY DEFAULT nextval('seq_pipeline_run_id'),
    month VARCHAR(7) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    -- 'running' until the run ends; a run that is still 'running' long after it started
    -- crashed without reaching its finally block.
    status VARCHAR NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    error VARCHAR,
    rows_inserted BIGINT,
    -- Row count of mart_daily_zone_trips after this run rebuilt it (NULL if it did not).
    mart_rows BIGINT
);
