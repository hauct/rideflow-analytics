{{ config(
    materialized='incremental',
    file_format='delta',
    location_root='s3a://rideflow/gold',
    unique_key=['request_date', 'request_hour', 'pickup_zone', 'city', 'cancellation_reason'],
    incremental_strategy='insert_overwrite',
    partition_by='request_date'
) }}

WITH fact_data AS (
    SELECT * FROM {{ ref('fact_trips') }}
    WHERE trip_status = 'cancelled'
    {% if is_incremental() %}
    AND request_date >= (
        SELECT COALESCE(MAX(request_date), '1970-01-01') 
        FROM {{ this }}
    )
    {% endif %}
),
cancelled_with_reason AS (
    SELECT
        request_time,
        DATE(request_time) AS request_date,
        HOUR(request_time) AS request_hour,
        pickup_zone,
        city,
        CASE
            WHEN (pickup_time IS NULL OR dropoff_time IS NULL) AND 
                 (request_time IS NOT NULL) THEN 'no_show'
            WHEN distance_km = 0 THEN 'zero_distance'
            ELSE 'other'
        END AS cancellation_reason
    FROM fact_data
)

SELECT
    request_date,
    request_hour,
    pickup_zone,
    city,
    cancellation_reason,
    COUNT(*) AS cancelled_trips_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY request_date, request_hour, pickup_zone, city) AS cancellation_percentage
FROM cancelled_with_reason
GROUP BY 
    request_date,
    request_hour,
    pickup_zone,
    city,
    cancellation_reason