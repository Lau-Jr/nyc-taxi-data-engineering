# Phase 9: fact insert, before vs after (repeated)

`python -m src.benchmark_fact_insert --repeats 3`: 726,002 fact rows (date_key < 20260108) loaded into a fresh on-disk database per run, with variants interleaved. Each timing includes a CHECKPOINT.

| Variant | Runs (s) | Median (s) | Rows/s (median) |
|---|---|---:|---:|
| before | 50.3, 59.1, 60.9 | 59.1 | 12,278 |
| after | 31.6, 31.6, 34.3 | 31.6 | 22,960 |

Speed-up (median): **1.9x**. Spread across runs: before 4.6s, after 1.3s (population std dev).
