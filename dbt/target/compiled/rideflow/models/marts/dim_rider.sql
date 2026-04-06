

-- Build rider dimension from trips + payments + ratings data
-- (No separate rider silver table exists; derive attributes from behavior)
WITH rider_trips AS (
    SELECT 
        rider_id,
        city,
        COUNT(trip_id) AS total_trips,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_trips,
        SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_trips,
        SUM(CASE WHEN status = 'completed' THEN fare_vnd ELSE 0 END) AS total_spent_vnd,
        MAX(ingest_date) AS last_trip_date
    FROM gold.stg_trips
    WHERE rider_id IS NOT NULL
    
    GROUP BY rider_id, city
),
rider_payments AS (
    SELECT 
        rider_id,
        -- Most frequently used payment method
        FIRST_VALUE(payment_method) OVER (
            PARTITION BY rider_id 
            ORDER BY cnt DESC
        ) AS preferred_payment
    FROM (
        SELECT 
            rider_id, 
            payment_method, 
            COUNT(*) AS cnt
        FROM gold.stg_payments
        WHERE payment_status = 'success'
        GROUP BY rider_id, payment_method
    ) pm
),
rider_payments_dedup AS (
    SELECT DISTINCT rider_id, preferred_payment
    FROM rider_payments
),
rider_ratings AS (
    SELECT 
        ratee_id AS rider_id,
        ROUND(AVG(stars), 2) AS avg_rating
    FROM gold.stg_ratings
    WHERE ratee_type = 'rider'
    GROUP BY ratee_id
)
SELECT 
    rt.rider_id,
    rt.city,
    rt.total_trips,
    rt.completed_trips,
    rt.cancelled_trips,
    rt.total_spent_vnd,
    COALESCE(rp.preferred_payment, 'unknown') AS preferred_payment,
    COALESCE(rr.avg_rating, 0) AS avg_rating,
    CASE 
        WHEN rt.total_spent_vnd >= 5000000 THEN 'vip'
        WHEN rt.completed_trips >= 10 THEN 'regular'
        ELSE 'casual'
    END AS segment,
    rt.last_trip_date
FROM rider_trips rt
LEFT JOIN rider_payments_dedup rp ON rt.rider_id = rp.rider_id
LEFT JOIN rider_ratings rr ON rt.rider_id = rr.rider_id