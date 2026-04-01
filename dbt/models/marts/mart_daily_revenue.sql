{{ config(
    materialized='incremental',
    file_format='delta',
    location_root='s3a://rideflow/gold',
    unique_key=['request_date', 'city'],
    incremental_strategy='insert_overwrite',
    partition_by='request_date'
) }}

WITH 
fact_data AS (
    SELECT * FROM {{ ref('fact_trips') }}
    {% if is_incremental() %}
    WHERE request_date >= (
        SELECT COALESCE(MAX(request_date), '1970-01-01') 
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    request_date,
    city,
    COUNT(trip_id) AS total_trips,
    SUM(CASE WHEN trip_status = 'completed' THEN 1 ELSE 0 END) AS completed_trips,
    SUM(CASE WHEN trip_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_trips,
    SUM(gmv_vnd) AS total_gmv_vnd,
    SUM(platform_revenue_vnd) AS total_platform_revenue_vnd,
    SUM(driver_earning_vnd) AS total_driver_earning_vnd,
    SUM(discount_vnd) AS total_discount_given
FROM fact_data
GROUP BY 
    request_date,
    city
