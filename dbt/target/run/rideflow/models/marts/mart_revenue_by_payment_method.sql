
    
      insert overwrite table gold.mart_revenue_by_payment_method
      partition (request_date)
    
    select `request_date`, `payment_method`, `completed_trips`, `total_gmv_vnd`, `total_platform_revenue_vnd`, `total_driver_earning_vnd`, `average_gmv_per_trip`, `total_discount_vnd` from mart_revenue_by_payment_method__dbt_tmp

