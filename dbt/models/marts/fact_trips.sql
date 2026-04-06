{{ config(
    materialized='incremental',
    file_format='delta',
    location_root='s3a://rideflow/gold',
    unique_key='trip_id',
    incremental_strategy='merge',
    partition_by='ingest_date'
) }}

WITH 
trips AS (
    SELECT * FROM {{ ref('stg_trips') }}
    {% if is_incremental() %}
    WHERE ingest_date >= (
        SELECT COALESCE(MAX(ingest_date), '1970-01-01') 
        FROM {{ this }}
    )
    {% endif %}
),
payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),
ratings AS (
    SELECT * FROM {{ ref('stg_ratings') }}
)

SELECT
    t.trip_id,
    t.driver_id,
    t.rider_id,
    t.status AS trip_status,
    t.city,
    t.pickup_zone,
    t.dropoff_zone,
    t.request_time,
    t.pickup_time,
    t.dropoff_time,
    t.distance_km,
    t.duration_min,
    
    -- Payment metrics
    p.payment_method,
    p.promo_code,
    COALESCE(p.fare_vnd, t.fare_vnd) AS base_fare_vnd,
    COALESCE(p.discount_vnd, 0) AS discount_vnd,
    COALESCE(p.final_amount_vnd, t.fare_vnd) AS gmv_vnd,
    COALESCE(p.platform_fee_vnd, CAST(t.fare_vnd * 0.2 AS INT)) AS platform_revenue_vnd,
    COALESCE(p.driver_earning_vnd, CAST(t.fare_vnd * 0.8 AS INT)) AS driver_earning_vnd,
    p.payment_status,
    
    -- Ratings
    rr.stars AS rider_rating_stars,
    rd.stars AS driver_rating_stars,
    
    -- Derived dimensional info
    DATE(t.request_time) AS request_date,
    HOUR(t.request_time) AS request_hour,
    
    t.ingest_date

FROM trips t
LEFT JOIN payments p ON t.trip_id = p.trip_id
LEFT JOIN ratings rr ON t.trip_id = rr.trip_id AND rr.rater_type = 'rider'
LEFT JOIN ratings rd ON t.trip_id = rd.trip_id AND rd.rater_type = 'driver'
