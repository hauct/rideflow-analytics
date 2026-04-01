{{ config(materialized='view') }}

SELECT
    trip_id,
    driver_id,
    rider_id,
    request_time,
    pickup_time,
    dropoff_time,
    status,
    city,
    pickup_zone,
    pickup_lat,
    pickup_lng,
    dropoff_zone,
    dropoff_lat,
    dropoff_lng,
    distance_km,
    duration_min,
    fare_vnd,
    ingest_date
FROM delta.`s3a://rideflow/silver/trips`
