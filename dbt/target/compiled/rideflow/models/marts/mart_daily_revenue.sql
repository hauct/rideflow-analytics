

WITH 
fact_data AS (
    SELECT * FROM gold.fact_trips
    
    WHERE request_date >= (
        SELECT COALESCE(MAX(request_date), '1970-01-01') 
        FROM gold.mart_daily_revenue
    )
    
)

SELECT
    request_date,
    city,
    COUNT(trip_id) AS total_trips,
    SUM(CASE WHEN trip_status = 'completed' THEN 1 ELSE 0 END) AS completed_trips,
    SUM(CASE WHEN trip_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_trips,
    SUM(gmv_vnd) AS total_gmv_vnd,
    SUM(platform_revenue_vnd) AS total_platform_revenue_vnd,
    SUM(driver_earning_vnd) AS total_driver_earning_vnd,
    SUM(discount_vnd) AS total_discount_given
FROM fact_data
GROUP BY 
    request_date,
    city