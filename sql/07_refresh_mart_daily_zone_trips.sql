-- Phase 7: the serving layer's one published table ("one clean tap").
-- Rebuilt in full at the end of every successful ingest run by
-- src/database.py::refresh_marts, which binds $refreshed_at. CREATE OR REPLACE makes the
-- refresh idempotent: rebuilding twice from the same fact_trip gives the same rows.
--
-- Grain: one row per pickup date x pickup zone. Every measure column is ADDITIVE (a count
-- or a sum), so consumers can roll up to any coarser grain (day, zone, whole month) and
-- then derive ratios. Ratios are never stored here, because an average of averages is
-- wrong. The ratio formulas live in one place, src/metrics.py, and are documented in
-- metrics.md.
--
-- Filters: fact_trip only holds rows that passed every hard check (quarantined rows never
-- reach it). Soft anomalies are INCLUDED in every measure and counted separately in
-- anomaly_trip_count. See metrics.md for why.
CREATE OR REPLACE TABLE mart_daily_zone_trips AS
SELECT
    d.full_date                                                          AS pickup_date,
    l.location_id                                                        AS pickup_location_id,
    COUNT(*)                                                             AS trip_count,
    COUNT(*) FILTER (WHERE f.is_anomaly)                                 AS anomaly_trip_count,
    CAST(SUM(f.total_amount) AS DECIMAL(18, 2))                          AS total_revenue,
    COUNT(*) FILTER (WHERE f.fare_amount > 0)                            AS paid_trip_count,
    CAST(COALESCE(SUM(f.fare_amount) FILTER (WHERE f.fare_amount > 0), 0)
         AS DECIMAL(18, 2))                                              AS paid_fare_sum,
    -- 0 < distance <= 100 mi: a handful of odometer errors (up to 269,098 mi in January)
    -- would otherwise dominate the distance sum. See avg_trip_distance in metrics.md.
    COUNT(*) FILTER (WHERE f.trip_distance > 0 AND f.trip_distance <= 100)
                                                                         AS distance_trip_count,
    CAST(COALESCE(SUM(f.trip_distance)
         FILTER (WHERE f.trip_distance > 0 AND f.trip_distance <= 100), 0)
         AS DECIMAL(18, 2))                                              AS distance_sum,
    CAST(COALESCE(SUM(f.fare_amount)
         FILTER (WHERE p.payment_type_id = 1 AND f.fare_amount > 0), 0)
         AS DECIMAL(18, 2))                                              AS card_fare_sum,
    CAST(COALESCE(SUM(f.tip_amount)
         FILTER (WHERE p.payment_type_id = 1 AND f.fare_amount > 0), 0)
         AS DECIMAL(18, 2))                                              AS card_tip_sum,
    MAX(f.pickup_datetime)                                               AS last_pickup_datetime,
    CAST($refreshed_at AS TIMESTAMP)                                     AS refreshed_at
FROM fact_trip f
JOIN dim_date d          ON f.date_key = d.date_key
LEFT JOIN dim_location l ON f.pickup_location_key = l.location_key
LEFT JOIN dim_payment p  ON f.payment_type_key = p.payment_type_key
GROUP BY d.full_date, l.location_id
ORDER BY pickup_date, pickup_location_id;
