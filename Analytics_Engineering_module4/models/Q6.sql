{{
    config(
        materialized='table'
    )
}}

WITH filtered_data AS (
    SELECT 
        fare_amount,
        service_type, 
        EXTRACT(YEAR FROM pickup_datetime) AS year, 
        EXTRACT(MONTH FROM pickup_datetime) AS month 
    FROM {{ ref('fact_trips') }} 
    WHERE fare_amount > 0 
      AND trip_distance > 0 
      AND payment_type_description IN ('Cash', 'Credit Card')
),
percentiles AS (
    SELECT 
        service_type,
        year, 
        month, 
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] AS p97,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] AS p95,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] AS p90 
    FROM filtered_data 
    GROUP BY service_type, year, month 
)
SELECT 
    service_type, 
    year, 
    month, 
    p97, 
    p95, 
    p90 
FROM percentiles 
WHERE year = 2020 AND month = 4 
    ORDER BY service_type