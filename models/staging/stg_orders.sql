with orders as (
    select * from {{ source("northwind","orders") }}
),
new_columns as (
    select
        orderid as order_id,
        customerid as customer_id,
        employeeid as employee_id,
        orderdate as order_date,
        requireddate as required_date,
        shippeddate as shipped_date,
        shipvia as shipper_id,
        freight,
        shipname,
        shipaddress as ship_address,
        shipcity as ship_city,
        shipregion as ship_region,
        shippostalcode as ship_postal_code,
        shipcountry as ship_country,
        current_timestamp() as dl_process_date,
        'I' as op,
        {{  dbt_utils.generate_surrogate_key([
            "order_id",
            "order_date",
            "required_date",
            "shipped_date",
            "shipper_id",
            "freight",
            "shipname",
            "ship_address",
            "ship_city",
            "ship_region",
            "ship_postal_code",
            "ship_country"
        ]) }}  as row_hash
    from orders
)
select * from new_columns