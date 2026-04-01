{{ config(
    materialized='incremental',
    file_format='delta',
    location_root='s3a://rideflow/gold',
    unique_key=['request_date', 'stars'],
    incremental_strategy='insert_overwrite',
    partition_by='request_date'
) }}

WITH fact_data AS (
    SELECT * FROM {{ ref('fact_trips') }}
    WHERE trip_status = 'completed'
    AND rider_rating_stars IS NOT NULL
    {% if is_incremental() %}
    AND request_date >= (
        SELECT COALESCE(MAX(request_date), '1970-01-01') 
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    DATE(request_time) AS request_date,
    rider_rating_stars AS stars,
    COUNT(*) AS rating_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE(request_time)) AS percentage_of_ratings
FROM fact_data
GROUP BY 
    DATE(request_time),
    rider_rating_stars