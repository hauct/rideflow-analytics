{{ config(
    materialized='incremental',
    file_format='delta',
    location_root='s3a://rideflow/gold',
    unique_key=['zone_id', 'city'],
    incremental_strategy='merge'
) }}

WITH zone_data AS (
    SELECT 
        pickup_zone AS zone_id,
        city,
        AVG(pickup_lat) AS lat,
        AVG(pickup_lng) AS lng
    FROM {{ ref('stg_trips') }}
    WHERE pickup_zone IS NOT NULL
    GROUP BY pickup_zone, city
    
    UNION ALL
    
    SELECT 
        dropoff_zone AS zone_id,
        city,
        AVG(dropoff_lat) AS lat,
        AVG(dropoff_lng) AS lng
    FROM {{ ref('stg_trips') }}
    WHERE dropoff_zone IS NOT NULL
    GROUP BY dropoff_zone, city
),
zone_agg AS (
    SELECT 
        zone_id,
        city,
        AVG(lat) AS lat,
        AVG(lng) AS lng
    FROM zone_data
    GROUP BY zone_id, city
)
SELECT 
    zone_id,
    city,
    ROUND(lat, 6) AS lat,
    ROUND(lng, 6) AS lng,
    CURRENT_TIMESTAMP() AS last_updated
FROM zone_agg