
    -- back compat for old kwarg name
  
  
  
      
          
              
              
          
              
              
          
      
  

  

  merge into gold.dim_zone as DBT_INTERNAL_DEST
      using dim_zone__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
                  DBT_INTERNAL_SOURCE.zone_id = DBT_INTERNAL_DEST.zone_id
               and 
                  DBT_INTERNAL_SOURCE.city = DBT_INTERNAL_DEST.city
              

      when matched then update set
         * 

      when not matched then insert *
