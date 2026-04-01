
    
      insert overwrite table gold.fact_trips
      partition (ingest_date)
    
    select `trip_id`, `driver_id`, `rider_id`, `trip_status`, `city`, `pickup_zone`, `dropoff_zone`, `request_time`, `pickup_time`, `dropoff_time`, `distance_km`, `duration_min`, `payment_method`, `promo_code`, `base_fare_vnd`, `discount_vnd`, `gmv_vnd`, `platform_revenue_vnd`, `driver_earning_vnd`, `payment_status`, `rider_rating_stars`, `driver_rating_stars`, `request_date`, `request_hour`, `ingest_date` from fact_trips__dbt_tmp

