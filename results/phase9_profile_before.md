# Phase 9 profile: before

`python -m src.profile_pipeline --month 2026-01 --label before`: one full ingest of 2026-01 (3,724,889 rows read, 3,679,812 inserted) into a fresh database. Run 2026-10-03 13:04 on Windows 10, Python 3.11.4, pandas 3.0.5, DuckDB 1.5.5.

| Step | Seconds | Share of run |
|---|---:|---:|
| read_parquet | 6.0 | 0.7% |
| completeness_log | 1.6 | 0.2% |
| add_trip_id | 267.9 | 32.0% |
| connect_and_load_code_maps | 0.1 | 0.0% |
| hard_checks | 39.6 | 4.7% |
| quarantine_write | 2.3 | 0.3% |
| soft_checks | 0.7 | 0.1% |
| dims_upsert | 169.2 | 20.2% |
| prepare_fact_rows | 2.8 | 0.3% |
| **fact_insert** | 338.6 | 40.5% |
| mart_refresh | 3.0 | 0.4% |
| other (connections, pipeline_run, logging) | 4.4 | 0.5% |
| **Total (wall clock)** | **836.1** | 100% |

Slowest step: **fact_insert**, 338.6 s (40% of the run).
