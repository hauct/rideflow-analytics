

-- Build driver dimension from trips + ratings data
-- (No separate driver silver table exists; derive attributes from behavior)
WITH driver_trips AS (
    SELECT 
        driver_id,
        city,
        COUNT(trip_id) AS total_trips,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_trips,
        SUM(distance_km) AS total_distance_km,
        SUM(duration_min) AS total_duration_min,
        SUM(CASE WHEN status = 'completed' THEN fare_vnd ELSE 0 END) AS total_fare_vnd,
        MAX(ingest_date) AS last_trip_date
    FROM gold.stg_trips
    WHERE driver_id IS NOT NULL
    
    AND ingest_date >= (
        SELECT COALESCE(MAX(last_trip_date), '1970-01-01') 
        FROM gold.dim_driver
    )
    
    GROUP BY driver_id, city
),
driver_ratings AS (
    SELECT 
        ratee_id AS driver_id,
        ROUND(AVG(stars), 2) AS avg_rating,
        COUNT(*) AS total_ratings
    FROM gold.stg_ratings
    WHERE ratee_type = 'driver'
    GROUP BY ratee_id
)
SELECT 
    dt.driver_id,
    dt.city,
    dt.total_trips,
    dt.completed_trips,
    dt.total_distance_km,
    dt.total_duration_min,
    dt.total_fare_vnd,
    COALESCE(dr.avg_rating, 0) AS avg_rating,
    COALESCE(dr.total_ratings, 0) AS total_ratings,
    CASE 
        WHEN dt.completed_trips >= 100 AND COALESCE(dr.avg_rating, 0) >= 4.5 THEN 'platinum'
        WHEN dt.completed_trips >= 50 AND COALESCE(dr.avg_rating, 0) >= 4.0 THEN 'gold'
        WHEN dt.completed_trips >= 20 THEN 'silver'
        ELSE 'bronze'
    END AS tier,
    dt.last_trip_date
FROM driver_trips dt
LEFT JOIN driver_ratings dr ON dt.driver_id = dr.driver_id