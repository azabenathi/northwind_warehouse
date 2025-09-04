with territories as (
    select * from {{ source("northwind","territories") }}
),
new_colums as (
    select
        TerritoryID as territory_id,
        TerritoryDescription as territory_description,
        RegionID as region_id,
        current_timestamp() as dl_process_date,
        'I' as op,
        {{ dbt_utils.generate_surrogate_key([
            "TerritoryID","TerritoryDescription","RegionID"]) }} as row_hash 
    from territories
)
select * from new_colums