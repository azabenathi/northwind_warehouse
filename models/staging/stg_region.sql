with region as (
    select * from {{ source("northwind","region") }}
),
new_colums as (
    select
        {{dbt_utils.generate_surrogate_key(["RegionID"])}} as hash_key,
        RegionID as region_id,
        RegionDescription as region_description,
        current_timestamp() as dl_process_date,
        'I' as op,
        {{ dbt_utils.generate_surrogate_key([
            "RegionID", "RegionDescription"]) }} as row_hash 
        
    from region
)
select * from new_colums