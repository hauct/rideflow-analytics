
  
    
        create or replace table gold.mart_revenue_by_payment_method
      
      
    using delta
      
      
      partitioned by (request_date)
      
      
    location 's3a://rideflow/gold/mart_revenue_by_payment_method'
      

      as
      

WITH fact_data AS (
    SELECT * FROM gold.fact_trips
    WHERE trip_status = 'completed'
    
)

SELECT
    DATE(request_time) AS request_date,
    payment_method,
    COUNT(trip_id) AS completed_trips,
    SUM(gmv_vnd) AS total_gmv_vnd,
    SUM(platform_revenue_vnd) AS total_platform_revenue_vnd,
    SUM(driver_earning_vnd) AS total_driver_earning_vnd,
    AVG(gmv_vnd) AS average_gmv_per_trip,
    SUM(discount_vnd) AS total_discount_vnd
FROM fact_data
GROUP BY 
    DATE(request_time),
    payment_method
  