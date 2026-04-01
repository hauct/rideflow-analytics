
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into gold.dim_rider as DBT_INTERNAL_DEST
      using dim_rider__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.rider_id = DBT_INTERNAL_DEST.rider_id
          

      when matched then update set
         * 

      when not matched then insert *
