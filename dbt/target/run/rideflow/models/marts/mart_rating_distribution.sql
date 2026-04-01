
    
      insert overwrite table gold.mart_rating_distribution
      partition (request_date)
    
    select `request_date`, `stars`, `rating_count`, `percentage_of_ratings` from mart_rating_distribution__dbt_tmp

