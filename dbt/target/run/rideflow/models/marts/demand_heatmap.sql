
    
      insert overwrite table gold.demand_heatmap
      partition (request_date)
    
    select `request_date`, `request_hour`, `pickup_zone`, `city`, `trip_count`, `completed_trips`, `cancelled_trips`, `avg_distance_km`, `avg_duration_min` from demand_heatmap__dbt_tmp

