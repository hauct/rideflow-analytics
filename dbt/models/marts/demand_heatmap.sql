{{ config(
    materialized='incremental',
    file_format='delta',
    location_root='s3a://rideflow/gold',
    unique_key=['request_date', 'request_hour', 'pickup_zone', 'city'],
    incremental_strategy='insert_overwrite',
    partition_by='request_date'
) }}

WITH fact_data AS (
    SELECT * FROM {{ ref('fact_trips') }}
    {% if is_incremental() %}
    WHERE request_date >= (
        SELECT COALESCE(MAX(request_date), '1970-01-01') 
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    DATE(request_time) AS request_date,
    HOUR(request_time) AS request_hour,
    pickup_zone,
    city,
    COUNT(trip_id) AS trip_count,
    SUM(CASE WHEN trip_status = 'completed' THEN 1 ELSE 0 END) AS completed_trips,
    SUM(CASE WHEN trip_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_trips,
    AVG(distance_km) AS avg_distance_km,
    AVG(duration_min) AS avg_duration_min
FROM fact_data
GROUP BY 
    DATE(request_time),
    HOUR(request_time),
    pickup_zone,
    city