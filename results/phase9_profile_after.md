# Phase 9 profile: after

`python -m src.profile_pipeline --month 2026-01 --label after`: one full ingest of 2026-01 (3,724,889 rows read, 3,679,812 inserted) into a fresh database. Run 2026-10-03 13:26 on Windows 10, Python 3.11.4, pandas 3.0.5, DuckDB 1.5.5.

| Step | Seconds | Share of run |
|---|---:|---:|
| read_parquet | 4.4 | 0.7% |
| completeness_log | 1.5 | 0.2% |
| **add_trip_id** | 247.1 | 38.9% |
| connect_and_load_code_maps | 0.1 | 0.0% |
| hard_checks | 42.6 | 6.7% |
| quarantine_write | 4.3 | 0.7% |
| soft_checks | 1.4 | 0.2% |
| dims_upsert | 163.2 | 25.7% |
| prepare_fact_rows | 2.8 | 0.4% |
| fact_insert | 161.8 | 25.5% |
| mart_refresh | 2.8 | 0.4% |
| other (connections, pipeline_run, logging) | 3.0 | 0.5% |
| **Total (wall clock)** | **634.9** | 100% |

Slowest step: **add_trip_id**, 247.1 s (39% of the run).
