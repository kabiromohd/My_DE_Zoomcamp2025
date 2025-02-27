{{
    config(
        materialized='table'
    )
}}

WITH trip_duration_data AS ( SELECT EXTRACT(YEAR FROM pickup_datetime) AS year,
 EXTRACT(MONTH FROM pickup_datetime) AS month, 
 pickup_location,
  TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration 
  FROM {{ ref('dim_fhv_trips') }} 
  WHERE pickup_locationid IN ('Newark Airport', 'SoHo', 'Yorkville East') AND EXTRACT(YEAR FROM pickup_datetime) = 2019 AND EXTRACT(MONTH FROM pickup_datetime) = 11 ),
   percentiles AS ( SELECT year, month, pickup_locationid, APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] AS p90 FROM trip_duration_data 
   GROUP BY year, month, pickup_locationid ) 
   SELECT year, month, pickup_locationid, p90 
   FROM percentiles ORDER BY pickup_locationid