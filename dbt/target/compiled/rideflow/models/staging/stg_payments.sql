

SELECT
    payment_id,
    trip_id,
    rider_id,
    payment_time,
    payment_method,
    fare_vnd,
    promo_code,
    discount_vnd,
    final_amount_vnd,
    payment_status,
    platform_fee_vnd,
    driver_earning_vnd,
    ingest_date
FROM delta.`s3a://rideflow/silver/payments`