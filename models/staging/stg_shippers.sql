with shippers as (
    select * from {{ source("northwind","shippers") }}
),
new_columns as (
    select
        shipperid as shipper_id,
        phone,
        companyname as company_name,
        current_timestamp() as dl_process_date,
        'I' as op,
        {{ dbt_utils.generate_surrogate_key([
            "shipper_id",
            "phone",
            "company_name"
        ]) }} as row_hash
    from shippers
)
select * from new_columns