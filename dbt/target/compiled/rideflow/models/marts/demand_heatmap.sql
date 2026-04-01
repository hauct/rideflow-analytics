

WITH fact_data AS (
    SELECT * FROM gold.fact_trips
    
    WHERE request_date >= (
        SELECT COALESCE(MAX(request_date), '1970-01-01') 
        FROM gold.demand_heatmap
    )
    
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