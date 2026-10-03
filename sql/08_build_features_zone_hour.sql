-- Phase 8: the ML feature table. Built by src/features.py::build_features, which binds the
-- parameters from config/features.toml. Rebuilt in full (CREATE OR REPLACE) on every build.
--
-- Task: forecast pickups per zone for every hour of day D.
-- Prediction moment: 00:00 on day D (an operator plans tomorrow's fleet at midnight).
-- Rule: every feature reads only trips that picked up BEFORE that midnight. Each one is
-- audited in docs/phase8/leakage_audit.md, and src/features.py re-checks the rule on
-- every build by rebuilding with the data cut off at midnight and comparing.
--
-- Parameters:
--   $grid_start, $grid_end   DATE       inclusive date range of the zone x hour grid
--   $cutoff                  TIMESTAMP  only trips with pickup_datetime < $cutoff are read
--                                       (far future for a normal build; midnight of a test
--                                       day for the truncation leakage check)
--   $holidays                DATE[]     public holidays inside the grid
--   $train_start, $validate_start, $test_start  DATE  split boundaries
CREATE OR REPLACE TABLE features_zone_hour AS
WITH hours AS (
    SELECT unnest(generate_series(
        CAST($grid_start AS TIMESTAMP),
        CAST(CAST($grid_end AS DATE) + 1 AS TIMESTAMP) - INTERVAL 1 HOUR,
        INTERVAL 1 HOUR)) AS hour_start
),
zones AS (
    SELECT location_id AS zone_id FROM dim_location
),
observed AS (
    SELECT l.location_id AS zone_id,
           date_trunc('hour', f.pickup_datetime) AS hour_start,
           COUNT(*) AS trips
    FROM fact_trip f
    JOIN dim_location l ON f.pickup_location_key = l.location_key
    WHERE f.pickup_datetime < CAST($cutoff AS TIMESTAMP)
    GROUP BY ALL
),
-- Complete grid: an hour with no pickups is a real 0, recorded explicitly, not a missing row.
grid AS (
    SELECT z.zone_id, h.hour_start,
           CAST(h.hour_start AS DATE) AS forecast_date,
           CAST(hour(h.hour_start) AS INTEGER) AS hour_of_day,
           COALESCE(o.trips, 0) AS trips
    FROM zones z
    CROSS JOIN hours h
    LEFT JOIN observed o ON o.zone_id = z.zone_id AND o.hour_start = h.hour_start
),
daily AS (
    SELECT zone_id, forecast_date, SUM(trips) AS day_trips
    FROM grid
    GROUP BY zone_id, forecast_date
),
daily_features AS (
    SELECT zone_id, forecast_date,
           LAG(day_trips, 1) OVER w AS zone_trips_prev_day,
           SUM(day_trips) OVER (w ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS trips_last_7d,
           SUM(day_trips) OVER (w ROWS BETWEEN 14 PRECEDING AND 8 PRECEDING) AS trips_prev_7d,
           COUNT(*) OVER (w ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS days_of_history
    FROM daily
    WINDOW w AS (PARTITION BY zone_id ORDER BY forecast_date)
),
hourly_features AS (
    -- Partitioned by zone AND hour of day: "1 preceding" is the same hour one day earlier.
    SELECT zone_id, hour_start, forecast_date, hour_of_day, trips,
           LAG(trips, 1) OVER w AS trips_same_hour_lag_1d,
           LAG(trips, 7) OVER w AS trips_same_hour_lag_7d,
           CASE WHEN COUNT(*) OVER (w ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) = 7
                THEN AVG(trips) OVER (w ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
           END AS trips_same_hour_mean_7d
    FROM grid
    WINDOW w AS (PARTITION BY zone_id, hour_of_day ORDER BY hour_start)
)
SELECT
    -- Keys
    h.zone_id,
    h.hour_start,
    h.forecast_date,
    CAST(h.forecast_date AS TIMESTAMP)                       AS prediction_made_at,

    -- Calendar features (known for any future hour)
    h.hour_of_day,
    CAST(isodow(h.forecast_date) AS INTEGER)                 AS day_of_week,
    isodow(h.forecast_date) IN (6, 7)                        AS is_weekend,
    list_contains(CAST($holidays AS DATE[]), h.forecast_date) AS is_public_holiday,

    -- History features (only days before forecast_date)
    h.trips_same_hour_lag_1d,
    h.trips_same_hour_lag_7d,
    h.trips_same_hour_mean_7d,
    d.zone_trips_prev_day,
    -- Smoothed ratio (+1 on both sides) so near-empty zones don't divide by zero.
    CASE WHEN d.days_of_history = 14
         THEN (d.trips_last_7d + 1.0) / (d.trips_prev_7d + 1.0)
    END                                                      AS zone_trips_trend_7d,
    d.days_of_history = 14                                   AS history_complete,

    -- Target: pickups in this zone during this hour
    h.trips                                                  AS target_trips,

    -- Split, fixed in config/features.toml before modelling
    CASE
        WHEN h.forecast_date < CAST($train_start AS DATE)    THEN 'warmup'
        WHEN h.forecast_date < CAST($validate_start AS DATE) THEN 'train'
        WHEN h.forecast_date < CAST($test_start AS DATE)     THEN 'validate'
        ELSE 'test'
    END                                                      AS split
FROM hourly_features h
JOIN daily_features d ON d.zone_id = h.zone_id AND d.forecast_date = h.forecast_date
ORDER BY h.zone_id, h.hour_start;
