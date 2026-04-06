
  
    
        create or replace table gold.mart_rating_distribution
      
      
    using delta
      
      
      partitioned by (request_date)
      
      
    location 's3a://rideflow/gold/mart_rating_distribution'
      

      as
      

WITH fact_data AS (
    SELECT * FROM gold.fact_trips
    WHERE trip_status = 'completed'
    AND rider_rating_stars IS NOT NULL
    
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
  