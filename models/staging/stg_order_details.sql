with order_details as (
    select * from {{ source("northwind","order_details") }}
),
new_columns as (
    select
        orderid as order_id,
        productid as product_id,
        unitprice as unit_price,
        quantity,
        discount,
        current_timestamp() as dl_process_date,
        'I' as op,
        {{ dbt_utils.generate_surrogate_key([
            "order_id",
            "product_id",
            "unit_price",
            "quantity",
            "discount"
        ])}} as row_hash
    from order_details
)
select * from new_columns