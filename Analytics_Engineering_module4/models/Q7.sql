{{
    config(
        materialized='table'
    )
}}

WITH trip_duration_data AS (
    SELECT 
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month, 
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration 
    FROM {{ ref('dim_fhv_trips') }} 
    WHERE pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East') 
      AND EXTRACT(YEAR FROM pickup_datetime) = 2019 
      AND EXTRACT(MONTH FROM pickup_datetime) = 11
),
percentiles AS (
    SELECT 
        year, 
        month, 
        pickup_locationid, 
        dropoff_locationid,
        APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] AS p90 
    FROM trip_duration_data 
    GROUP BY year, month, pickup_locationid, dropoff_locationid
),
ranked_percentiles AS (
    SELECT 
        year, 
        month, 
        pickup_locationid, 
        dropoff_locationid, 
        p90,
        ROW_NUMBER() OVER (PARTITION BY year, month, pickup_locationid ORDER BY p90 DESC) AS rank
    FROM percentiles
)
SELECT 
    year, 
    month, 
    pickup_locationid, 
    dropoff_locationid, 
    p90 
FROM ranked_percentiles 
WHERE rank = 2
ORDER BY pickup_locationid, dropoff_locationid