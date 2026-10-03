# Metric definitions

**Version 1.0** · owner: Lau-Jr (data team of one) · implemented in
[`src/metrics.py`](src/metrics.py) (`METRICS_VERSION`) on top of the published table
`mart_daily_zone_trips` ([`sql/07_refresh_mart_daily_zone_trips.sql`](sql/07_refresh_mart_daily_zone_trips.sql)).

Every published number is defined here once and computed once, in the pipeline. Consumers
(the [dashboard](streamlit_app.py), notebooks, future reports) call
`metrics.summarise(mart, by=...)`. They never compute these numbers themselves and never
read `fact_trip` or raw files.

## The published table

| | |
|---|---|
| Table | `mart_daily_zone_trips` in `data/processed/taxi.duckdb` |
| Grain | one row per **pickup date × pickup zone** (`pickup_date`, `pickup_location_id`) |
| Source | `fact_trip` ⋈ `dim_date`, `dim_location`, `dim_payment` |
| Refresh | rebuilt in full by `src/ingest.py` at the end of **every successful ingest run** |
| Freshness | `MAX(last_pickup_datetime)` = latest trip in the data; `refreshed_at` = when the pipeline rebuilt it; run status in `pipeline_run`. Computed by `src/serving.py::get_freshness` |
| Columns | additive only: `trip_count`, `anomaly_trip_count`, `total_revenue`, `paid_trip_count`, `paid_fare_sum`, `distance_trip_count`, `distance_sum`, `card_fare_sum`, `card_tip_sum`, plus `last_pickup_datetime`, `refreshed_at` |

The mart stores counts and sums, never ratios. `summarise()` rolls the rows up to whatever
grain the consumer asks for (day, zone, whole period), and only then divides. A monthly
average fare is therefore total fares ÷ total paid trips, not the mean of 31 daily
averages.

## Filters that apply to every metric

1. **Quarantined rows are excluded.** Rows that fail a hard check (Unit 6,
   `src/validation.py::HARD_REJECT_CHECKS`) never reach `fact_trip`. In January 2026 that
   is 45,077 of 3,724,889 rows (1.2%), almost all `dropoff <= pickup`.
2. **Soft anomalies are included.** Rows with `is_anomaly = TRUE` are real trips with an
   implausible or missing *optional* field, so they count in every metric below. Their
   share is published as its own metric (`anomaly_share`).

Why include them: in January, 1,171,219 trips (31.8%) carry the anomaly flag, and they
account for $34.9M of $107.5M total revenue. 1,088,010 of them are flagged only because
`passenger_count` is null. All of those are `payment_type = 0` ("not provided") rows, in
which TLC leaves five optional columns empty together. Dropping them would understate
demand and revenue by about a third. That is exactly what the Phase 2 queries 1 and 2 in
`sql/04_analytical_queries.sql` did, because they filter `is_anomaly = FALSE`. Those
queries are kept as the Phase 2 deliverable, but they are **not** the published numbers.

## Metrics

| Metric | Formula (over the rows in scope) | Metric-specific filter | Unit | Notes |
|---|---|---|---|---|
| `trip_count` | `SUM(trip_count)` | none | trips | One trip = one `fact_trip` row (unique `trip_id`). |
| `total_revenue` | `SUM(total_amount)` | none | USD | Everything the passenger was charged (fare, surcharges, tolls, tips). **Net of refunds**: negative reversal rows are included, so a charge and its reversal cancel out. |
| `avg_fare` | `SUM(paid_fare_sum) / SUM(paid_trip_count)` | `fare_amount > 0` | USD per trip | Metered fare only, no extras or tips. Zero and negative fares (reversals, no-charge trips) are left out so they don't drag the average down. |
| `avg_trip_distance` | `SUM(distance_sum) / SUM(distance_trip_count)` | `0 < trip_distance <= 100` | miles per trip | Zero-distance trips (124,574 in January) and trips over 100 miles (161 in January, up to 269,098 mi) are left out of the average but still count in `trip_count`. Those 161 odometer errors hold 48% of all recorded miles: with them the January average is 6.73 mi, without them 3.50 mi (median 1.90 mi). |
| `card_tip_rate` | `SUM(card_tip_sum) / SUM(card_fare_sum)` | `payment_type_id = 1` (credit card) and `fare_amount > 0` | ratio (shown as %) | TLC records tips only for card payments. Cash tips are 0 in the data, so including cash trips would understate tipping. Empty (not 0) when there are no card fares in scope. |
| `anomaly_share` | `SUM(anomaly_trip_count) / SUM(trip_count)` | none | ratio (shown as %) | Share of trips flagged by any soft check. A quality signal, published so consumers can see how much of each number rests on imperfect rows. |

**Grain of every metric:** whatever the consumer rolls up to, from a single date × zone
cell (the mart's grain) up to the whole period. The dashboard shows it per period
(KPI row), per pickup day (trend chart), and per pickup zone (top-10 chart).

**Pickup zone** is the TLC `PULocationID` (1–265). No zone-name lookup ships with this
dataset, so zones are shown as ids (`Zone 132`).

## Changing a metric

1. Edit the formula in `src/metrics.py`, and the mart in `sql/07` if a new additive column
   is needed.
2. Update the row in this file and bump the version: **minor** for a new metric, **major**
   when an existing number changes meaning.
3. Add a changelog line below. The next ingest run republishes the mart, and every consumer
   picks up the change together.

## Changelog

| Version | Date | Change |
|---|---|---|
| 1.0 | 2026-10-03 | First published definitions: `trip_count`, `total_revenue`, `avg_fare`, `avg_trip_distance`, `card_tip_rate`, `anomaly_share`. Soft anomalies included (see above). |
