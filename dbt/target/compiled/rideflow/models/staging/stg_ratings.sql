

SELECT
    rating_id,
    trip_id,
    rater_id,
    ratee_id,
    rater_type,
    ratee_type,
    stars,
    tags,
    rated_at,
    ingest_date
FROM delta.`s3a://rideflow/silver/ratings`