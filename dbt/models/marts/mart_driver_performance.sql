{{ config(
    materialized='incremental',
    file_format='delta',
    location_root='s3a://rideflow/gold',
    unique_key=['year_week', 'driver_id', 'city'],
    incremental_strategy='insert_overwrite',
    partition_by='year_week'
) }}

WITH fact_data AS (
    SELECT * FROM {{ ref('fact_trips') }}
    WHERE trip_status = 'completed'
    {% if is_incremental() %}
    AND request_date >= (
        SELECT COALESCE(MAX(request_date), '1970-01-01') 
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    YEAR(request_time) * 100 + WEEKOFYEAR(request_time) AS year_week,
    YEAR(request_time) AS year,
    WEEKOFYEAR(request_time) AS week_number,
    driver_id,
    city,
    MIN(request_date) AS request_date,
    COUNT(trip_id) AS trips_completed,
    SUM(distance_km) AS total_distance_km,
    SUM(duration_min) AS total_duration_min,
    SUM(driver_earning_vnd) AS weekly_earnings_vnd,
    AVG(rider_rating_stars) AS avg_stars_received,
    COUNT(rider_rating_stars) AS total_ratings_received
FROM fact_data
GROUP BY 
    YEAR(request_time) * 100 + WEEKOFYEAR(request_time),
    YEAR(request_time),
    WEEKOFYEAR(request_time),
    driver_id,
    city
