
    
      insert overwrite table gold.dim_time
      partition (date)
    
    select `time_id`, `date`, `hour`, `minute`, `day_of_week`, `day_of_month`, `month`, `year`, `period_of_day` from dim_time__dbt_tmp

