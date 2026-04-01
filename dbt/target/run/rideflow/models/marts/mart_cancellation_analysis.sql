
    
      insert overwrite table gold.mart_cancellation_analysis
      partition (request_date)
    
    select `request_date`, `request_hour`, `pickup_zone`, `city`, `cancellation_reason`, `cancelled_trips_count`, `cancellation_percentage` from mart_cancellation_analysis__dbt_tmp

