
    
      insert overwrite table gold.mart_driver_performance
      partition (year_week)
    
    select `year_week`, `year`, `week_number`, `driver_id`, `city`, `request_date`, `trips_completed`, `total_distance_km`, `total_duration_min`, `weekly_earnings_vnd`, `avg_stars_received`, `total_ratings_received` from mart_driver_performance__dbt_tmp

