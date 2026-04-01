
    
      insert overwrite table gold.mart_daily_revenue
      partition (request_date)
    
    select `request_date`, `city`, `total_trips`, `completed_trips`, `cancelled_trips`, `total_gmv_vnd`, `total_platform_revenue_vnd`, `total_driver_earning_vnd`, `total_discount_given` from mart_daily_revenue__dbt_tmp

