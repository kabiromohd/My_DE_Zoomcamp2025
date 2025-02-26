{{
    config(
        materialized='table'
    )
}}

WITH quarterly_revenue AS ( SELECT EXTRACT(YEAR FROM pickup_datetime) AS year,
 EXTRACT(QUARTER FROM pickup_datetime) AS quarter, service_type,
  SUM(total_amount) AS total_revenue 
  FROM {{ ref('fact_trips') }} 
  WHERE pickup_datetime >= '2019-01-01' 
  GROUP BY year, quarter, service_type ), 
  revenue_growth AS ( SELECT this_year.year, 
  this_year.quarter, 
  this_year.service_type,
   this_year.total_revenue AS current_year_revenue,
    COALESCE(last_year.total_revenue, 0) AS last_year_revenue,
     (this_year.total_revenue - COALESCE(last_year.total_revenue, 0)) / COALESCE(last_year.total_revenue, 1) * 100 AS yoy_growth FROM quarterly_revenue AS this_year LEFT JOIN quarterly_revenue AS last_year ON this_year.service_type = last_year.service_type AND this_year.quarter = last_year.quarter AND this_year.year = last_year.year + 1 ) SELECT year, 
     quarter, service_type, yoy_growth 
     FROM revenue_growth 
     ORDER BY year, quarter, service_type