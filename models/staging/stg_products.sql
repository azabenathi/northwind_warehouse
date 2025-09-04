with products as (
    select * from {{ source("northwind","products") }}
),
new_columns as (
    select
        productid as product_id,
        productname as product_name,
        supplierid as supplier_id,
        categoryid as category_id,
        quantityperunit as quantity_per_unit,
        unitprice as unit_price,
        unitsinstock as units_in_stock,
        unitsonorder as units_on_order,
        reorderlevel as reorder_level,
        discontinued,
        current_timestamp() as dl_process_date,
        'I' as op,
        {{ dbt_utils.generate_surrogate_key([
            "product_id",
            "product_name",
            "supplier_id",
            "category_id",
            "quantity_per_unit",
            "unit_price",
            "units_in_stock",
            "units_on_order",
            "reorder_level",
            "discontinued"
        ])}} as row_hash
    from products
)
select * from new_columns