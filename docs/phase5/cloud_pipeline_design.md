# Phase 5 — Cloud Pipeline Design (paper design)

https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1sleaning-de-and-sys-admin!2sNYC_YELLOW_TAX!3strips_sample

**Dataset:** NYC TLC Yellow Taxi trip records, monthly parquet drops.
**Warehouse:** BigQuery (same star schema as `sql/`, adapted for a cloud-native source).

## Architecture

```
 TLC public bucket                                                    ┌─────────────────┐
 (new parquet file          ┌──────────────┐      ┌──────────────┐    │  Looker Studio /  │
 released monthly)  ───────▶│ Cloud         │─────▶│  GCS: raw     │    │  notebook (BI)     │
                     [1]     │ Scheduler +   │  [2] │  bucket       │    └────────▲─────────┘
                             │ Cloud Function│      │ (landing zone)│             │ [7]
                             └──────────────┘      └──────┬────────┘             │
                                                            │ [3]                 │
                                                            ▼                     │
                                                    ┌──────────────┐              │
                                                    │  Dataflow job │              │
                                                    │  (validate +  │              │
                                                    │  transform,   │              │
                                                    │  = src/       │              │
                                                    │  validation.py│              │
                                                    │  + transform  │              │
                                                    │  .py logic)   │              │
                                                    └──────┬───────┘               │
                                              [4] valid │      │ [5] rejected      │
                                                         ▼      ▼                  │
                                              ┌──────────────┐ ┌───────────────┐    │
                                              │  BigQuery:    │ │ GCS: rejected  │    │
                                              │  star schema  │ │ rows + reason  │    │
                                              │  (fact_trip + │ │ (audit trail)  │    │
                                              │  dims),       │ └───────────────┘    │
                                              │  partitioned +│                      │
                                              │  clustered    │──────────────────────┘
                                              └──────┬───────┘         [6]
                                                     │
                                                     ▼
                                          ┌──────────────────────┐
                                          │ Cloud Logging /        │
                                          │ Monitoring (job status,│
                                          │ row counts, alerting)  │
                                          └──────────────────────┘
                                                     [8]
```

## Boxes, arrows, and their cost drivers

| # | Box / arrow | Role | Cost driver | Cost note |
|---|---|---|---|---|
| 1 | Cloud Scheduler → Cloud Function | Monthly trigger that downloads the new TLC parquet file | Function invocation + compute time (~seconds/month) | Effectively free at this cadence — Cloud Functions free tier covers 2M invocations/month; one run/month is negligible. |
| 2 | GCS raw bucket (landing zone) | Stores the untouched source parquet, one object per month | **Storage**, billed per GB/month | ~62 MB/month × Standard storage (~$0.02/GB/mo US) ≈ fractions of a cent/month; grows linearly — cheap even at years of history. |
| 3 | Raw bucket → Dataflow | Triggers the validate/transform job (e.g. via Eventarc on object finalize) | Included in Dataflow job cost below | No separate charge — event trigger itself is free. |
| 4 | Dataflow job (validate + transform) | Runs the same hard-reject/soft-anomaly split and `trip_id`/`date_key` derivation as `src/validation.py` + `src/transform.py`, at cloud scale | **Compute**, billed per vCPU-hour + GB-hour of worker time while the job runs | A batch job over ~3.7M rows finishes in minutes on a small worker pool — a few dollars at most per monthly run; this is the box to autoscale down or batch-schedule off-peak to control cost. |
| 5 | Dataflow → GCS rejected bucket | Writes hard-rejected rows with a reason code (mirrors `logs/rejected_<month>.csv`) | Storage only, same rate as box 2 | Rejected rows are ~1% of volume — negligible additional storage. |
| 6 | Dataflow → BigQuery (load) | Loads validated, transformed rows into the `fact_trip`/dimension tables | **Free** — BigQuery load jobs (batch load from GCS) are not billed for the load itself | The only BigQuery cost here is storage (below), not the act of loading. |
| — | BigQuery table storage | Holds `fact_trip` + dimensions, **partitioned by `pickup_datetime` (day) and clustered by `PULocationID`, `vendor_key`** | **Storage**, billed per GB/month; **query cost is proportional to bytes scanned**, not row count | ~350 MB/month of fact rows × $0.02/GB active storage ≈ a fraction of a cent/month; the real lever is query cost below — partition+cluster is what keeps it small. |
| 7 | BigQuery ← BI tool / notebook queries | Analysts run aggregate queries (Looker Studio dashboard, ad-hoc SQL, this course's Phase 2 analytical queries) | **On-demand query pricing: $6.25/TB scanned** (first 1 TB/month free) — **this is the same "bytes processed" number the sandbox query showed** | A query that filters by date and scans one partition instead of the whole table can cut bytes-scanned (and cost) by >30× — directly reproduces the sandbox lesson at production scale. A dashboard re-running an unpartitioned full-table scan on every refresh is the single most common cost blowout in this architecture. |
| 8 | Cloud Logging / Monitoring | Row-count and failure alerting for every stage (rows read/valid/rejected/inserted — same fields the local pipeline already logs) | Ingest volume for logs | Free tier covers 50 GB logs/month, far more than this pipeline generates; alerting policies are free, only notification channels (e.g. SMS) can incur cost. |

## Why this shape

The sandbox query's **bytes processed** figure is the direct analogue of box 7 above: BigQuery
bills on-demand queries by data scanned, not rows returned or query complexity. The single
biggest design lever in this whole pipeline is therefore **partitioning `fact_trip` by pickup
date and clustering by the columns most queries filter on** — every other box (Scheduler,
Dataflow, Logging) costs cents to a few dollars a month at this data volume; an unpartitioned
table hit repeatedly by a live dashboard is the box that can actually get expensive.

## Sandbox result

- Sample loaded: `data/samples/yellow_tripdata_2026-01_sample100k.csv` (100,000 rows, ~10.3 MB) → table `leaning-de-and-sys-admin.NYC_YELLOW_TAX.trips_sample`, region `africa-south1`.
- Query run:
  ```sql
  SELECT PULocationID, COUNT(*) trip_count, ROUND(SUM(total_amount),2) revenue
  FROM `leaning-de-and-sys-admin.NYC_YELLOW_TAX.trips_sample`
  GROUP BY PULocationID
  ORDER BY revenue DESC
  LIMIT 10;
  ```
  Top result: PULocationID 132, 4,117 trips, $290,902.56 revenue (job completed in 261 ms).
- **Bytes processed: 1.53 MB.** **Bytes billed: 10 MB.**
- Bytes billed is 6.5× bytes processed here — BigQuery's on-demand pricing has a **10 MB
  minimum billing quantum per query**, so any query this small gets billed as if it scanned
  10 MB regardless of how little it actually touched. At production scale (box 7 above) this
  floor is irrelevant — a full-table scan on an unpartitioned multi-GB `fact_trip` dwarfs it —
  but it matters for exactly this kind of small, frequent exploratory query: a dashboard
  firing hundreds of small filtered queries per day pays the 10 MB floor on *each one*, which
  is why batching/caching small repeated queries (or using a materialized view) can matter
  even before the table gets large.
- Screenshot: attached separately per the lab deliverable (job information + query/results panels).
