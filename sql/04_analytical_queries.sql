-- Phase 2 demonstration queries: each joins fact_trip against at least one dimension,
-- to answer "why not one big table?" — the descriptive attributes (payment/vendor/date
-- descriptions) live once in the dimension and are looked up, not repeated per trip.

-- 1. Revenue and trip volume by day of week (excludes flagged anomalies from Problem B).
SELECT
    d.day_name,
    COUNT(*)                       AS trip_count,
    ROUND(SUM(f.total_amount), 2)  AS total_revenue,
    ROUND(AVG(f.total_amount), 2)  AS avg_fare
FROM fact_trip f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.is_anomaly = FALSE
GROUP BY d.day_name
ORDER BY total_revenue DESC;

-- 2. Trip volume and revenue by pickup zone, top 10 by revenue.
SELECT
    l.location_id       AS pickup_location_id,
    COUNT(*)             AS trip_count,
    ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM fact_trip f
JOIN dim_location l ON f.pickup_location_key = l.location_key
WHERE f.is_anomaly = FALSE
GROUP BY l.location_id
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Payment method mix by vendor — demonstrates a three-way fact/dimension join.
SELECT
    v.vendor_name,
    p.payment_description,
    COUNT(*)                      AS trip_count,
    ROUND(AVG(f.tip_amount), 2)   AS avg_tip
FROM fact_trip f
JOIN dim_vendor v  ON f.vendor_key = v.vendor_key
JOIN dim_payment p ON f.payment_type_key = p.payment_type_key
GROUP BY v.vendor_name, p.payment_description
ORDER BY v.vendor_name, trip_count DESC;

-- 4. Average fare by rate type, including the "Unknown" (rate_code_id = 99) member
--    that a plain WHERE RatecodeID IN (1..6) filter on the raw file would have dropped.
SELECT
    r.rate_description,
    COUNT(*)                       AS trip_count,
    ROUND(AVG(f.fare_amount), 2)   AS avg_fare_amount
FROM fact_trip f
JOIN dim_rate_code r ON f.rate_code_key = r.rate_code_key
GROUP BY r.rate_description
ORDER BY trip_count DESC;
