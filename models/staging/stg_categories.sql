with categories as (
    select * from {{ source("northwind","categories") }}
),
new_columns as (
    select
        categoryid as category_id,
        categoryname as category_name,
        description
        picture,
        current_timestamp() as dl_process_date,
        'I' as op,
        {{ dbt_utils.generate_surrogate_key([
            "category_id",
            "category_name",
            "description",
        ]) }} as row_hash 
    from categories
)

select * from new_columns