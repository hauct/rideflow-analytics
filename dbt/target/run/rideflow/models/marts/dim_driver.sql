
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into gold.dim_driver as DBT_INTERNAL_DEST
      using dim_driver__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.driver_id = DBT_INTERNAL_DEST.driver_id
          

      when matched then update set
         * 

      when not matched then insert *
