# Phase 9 — Find it. Fix it. Prove it.

## 1. Find it: profile of one full ingest

`src/ingest.py` now times every step (`StepTimer`): a `[timing]` line per step in the log,
and `step_seconds` in the run summary. `python -m src.profile_pipeline` runs one full month
(January 2026: 3,724,889 rows read, 3,679,812 inserted) into a **fresh** scratch database,
so every profile starts from the same state. Environment: Windows 10, Python 3.11.4,
pandas 3.0.5, DuckDB 1.5.5. Full tables: [before](../../results/phase9_profile_before.md),
[after](../../results/phase9_profile_after.md).

| Step | Before (s) | Share | After (s) | Share |
|---|---:|---:|---:|---:|
| read_parquet | 6.0 | 0.7% | 4.4 | 0.7% |
| completeness_log | 1.6 | 0.2% | 1.5 | 0.2% |
| add_trip_id | 267.9 | 32.0% | 247.1 | **38.9%** ← new bottleneck |
| connect_and_load_code_maps | 0.1 | 0.0% | 0.1 | 0.0% |
| hard_checks | 39.6 | 4.7% | 42.6 | 6.7% |
| quarantine_write | 2.3 | 0.3% | 4.3 | 0.7% |
| soft_checks | 0.7 | 0.1% | 1.4 | 0.2% |
| dims_upsert | 169.2 | 20.2% | 163.2 | 25.7% |
| prepare_fact_rows | 2.8 | 0.3% | 2.8 | 0.4% |
| **fact_insert** | **338.6** | **40.5%** ← bottleneck | **161.8** | 25.5% |
| mart_refresh | 3.0 | 0.4% | 2.8 | 0.4% |
| other (connections, run log) | 4.4 | 0.5% | 3.0 | 0.5% |
| **Total (wall clock)** | **836.1** (13.9 min) | | **634.9** (10.6 min) | |

**The bottleneck was `fact_insert`: 338.6 s, 40% of the run.** Two findings from measuring
rather than guessing:

- `dims_upsert` took **169 s**. Nobody suspected it: in the old logs it was hidden inside
  "the load". It builds `date_key` with `strftime` on 3.7 million rows to produce 31
  distinct dates.
- My first candidate fix for the insert, **sorting the batch by `trip_id`** so the index is
  built from ordered keys, measured *slower* (49.8 s against 52.0 s, plus 6 s to sort, on
  844k rows). It was dropped without being shipped.

## 2. Diagnose: what inside the insert costs the time

`EXPLAIN ANALYZE` on a 319k-row insert showed 13.0 s in total, but only about 6 s in the
plan's own operators (MERGE_INTO 3.6 s, HASH_GROUP_BY 1.7 s, casts and scans under 0.5 s).
The rest goes on **constraint enforcement**, which runs for every row as it is written.
`fact_trip` declared a UNIQUE index on `trip_id` and **six FOREIGN KEY constraints**, each
an index lookup into a dimension table, for every inserted row. Loading 844k rows into an
on-disk database, one variant at a time:

| `fact_trip` schema | First load | Replay |
|---|---:|---:|
| UNIQUE + 6 FKs (as it was) | 71.4 s | 3.0 s |
| no UNIQUE (anti-join instead), FKs kept | 46.0 s | 1.1 s |
| **UNIQUE kept, no FKs** | **27.1 s** | 3.1 s |

## 3. Fix it: one deliberate change

**Remove the six FOREIGN KEY constraints from `fact_trip` (`sql/03_create_fact.sql`), and
enforce the same rule once per batch instead of once per row.**

- `src/database.py::check_fact_foreign_keys` runs one set-based anti-join per key column
  over the whole batch. If any key has no matching dimension row, it raises
  `ReferentialIntegrityError` **before anything is written**, so the whole batch is
  refused. NULL keys are allowed, as they were under the constraint (`rate_code_key`).
- The UNIQUE constraint on `trip_id` is **kept**. It is what makes replaying a month a
  no-op (Phase 3), and it is the single largest guarantee in the warehouse.
- Existing databases are migrated once by `database.drop_fact_foreign_keys`, called from
  `ensure_schema`. DuckDB cannot drop a constraint in place, so the table is rebuilt from
  the new DDL in one transaction, keeping every `trip_key`. On the real database this took
  246 s, once. Before and after: 7,039,065 rows, $208,839,100.46, max `trip_key` 14,398,689,
  7,039,065 distinct `trip_id`s.

## 4. Prove it: before / after

**Full pipeline, one run each** (January, fresh database):

| | Before | After | Change |
|---|---:|---:|---:|
| `fact_insert` step | 338.6 s | 161.8 s | **2.1× faster** (−176.8 s) |
| Whole run | 836.1 s | 634.9 s | **−24%** (−201 s) |

**The fixed step alone, repeated** (`python -m src.benchmark_fact_insert --repeats 3`: 1–7
January, 726,002 rows, fresh on-disk database per run, variants interleaved, CHECKPOINT
inside the timing; [results](../../results/phase9_fact_insert_benchmark.md)):

| Variant | Runs (s) | Median | Rows/s |
|---|---|---:|---:|
| before (FKs, `ON CONFLICT`) | 50.3, 59.1, 60.9 | 59.1 s | 12,278 |
| after (no FKs, batch check + `ON CONFLICT`) | 31.6, 31.6, 34.3 | 31.6 s | 22,960 |

**1.9× faster at the median, and steadier** (spread 4.6 s before, 1.3 s after). The "after"
timings include the new batch check, so the replacement is not getting a free ride.

**Same answers:** all 47 tests pass. Ten new tests in `tests/test_performance.py` cover:
- every kind of unknown key refusing the whole batch;
- a replay still inserting 0 rows;
- migration keeping every row;
- every run reporting its step timings.

The Phase 8 feature snapshot, rebuilt from the migrated warehouse, is **byte-identical**
(sha256 `fc6a6e6b9c35…`), so not one downstream number moved.

## Why the fix worked

A foreign-key constraint is an index lookup the database makes **for every inserted row**:
"does this `date_key` exist in `dim_date`?", then the same for the two locations, the
vendor, the payment type and the rate code. That is six probes per row, 22 million probes
for one month, all repeating a question the pipeline had already answered. The dimension
rows are upserted from the same batch just before, and the vendor, payment and rate codes
are mapped from the seeded dimensions, with unknown codes quarantined by the Unit 6 hard
checks. Slide 7 warns that *"every index must be updated on every write"*. A constraint is
the same trade-off: correctness bought with per-row cost on every write. Moving the check
from **per row** to **per batch** keeps the guarantee, because a batch with an orphan key
is still refused whole. It replaces 22 million random index probes with six hash
anti-joins that each read the batch once. The UNIQUE index was left alone, because it is
the one constraint that does something no upstream step can: it stops a replayed month
from loading twice.

## Costs and what's next

- **The guarantee now lives in code, not in the schema.** A write that bypasses
  `insert_fact_trips` (for example, someone typing `INSERT` by hand) is no longer checked.
  It is a deliberate trade-off for a single-writer pipeline, and it reverses the Phase 2
  design choice ([schema_design.md](../phase2/schema_design.md), updated to say so).
- **Fix it, and a new one takes its place** (slide 3). The bottleneck is now
  **`add_trip_id`: 247 s, 39% of the run**. It builds the key string with `.map(str)` row
  by row in Python. Vectorising it is the next fix. It must produce byte-identical IDs or
  the warehouse needs one rebuild: a DuckDB version reproduced only 2.1M of 7.1M stored
  IDs. After that, `dims_upsert` (163 s) should take a few seconds once `date_key` is
  computed on distinct dates instead of every row.
- **What wasn't measured:** memory use, and runs on another machine. Every number above
  comes from a single Windows laptop with other applications open; the repeated benchmark
  is there to show the spread.
