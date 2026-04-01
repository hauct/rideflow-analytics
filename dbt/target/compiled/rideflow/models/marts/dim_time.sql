

WITH time_data AS (
    SELECT 
        DISTINCT
        request_time AS time_id,
        DATE(request_time) AS date,
        HOUR(request_time) AS hour,
        MINUTE(request_time) AS minute,
        DAYOFWEEK(request_time) AS day_of_week,
        DAYOFMONTH(request_time) AS day_of_month,
        MONTH(request_time) AS month,
        YEAR(request_time) AS year,
        CASE 
            WHEN HOUR(request_time) IN (7,8,9,17,18,19) THEN 'peak'
            ELSE 'off_peak'
        END AS period_of_day
    FROM gold.fact_trips
    
    WHERE DATE(request_time) >= (
        SELECT COALESCE(MAX(date), '1970-01-01') 
        FROM gold.dim_time
    )
    
)
SELECT 
    time_id,
    date,
    hour,
    minute,
    day_of_week,
    day_of_month,
    month,
    year,
    period_of_day
FROM time_data