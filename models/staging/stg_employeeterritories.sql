with employeeterritories as (
    select * from {{ source("northwind","employeeterritories") }}
),
new_colums as (
    select
        {{dbt_utils.generate_surrogate_key(["EmployeeID","TerritoryID"])}} as hash_key,
        EmployeeID as employee_id,
        TerritoryID as territory_id,
        current_timestamp() as dl_process_date,
        'I' as op,
        {{ dbt_utils.generate_surrogate_key([
            "EmployeeID","TerritoryID"]) }} as row_hash 
    from employeeterritories
)
select * from new_colums