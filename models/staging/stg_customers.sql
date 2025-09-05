with customers as (
    select * from {{ source("northwind","customers") }}
),
new_columns as (
    select
        customerid as customer_id,
        companyname as company_name,
        contactname as contact_name,
        contacttitle as contact_title,
        address,
        city,
        region,
        postalcode as postal_code,
        country,
        phone,
        fax,
        current_timestamp() as dl_process_date,
        'I' as op,
        {{ dbt_utils.generate_surrogate_key([
            "customer_id",
            "company_name",
            "contact_name",
            "contact_title",
            "address",
            "city",
            "region",
            "contact_title",
            "country"
        ])}} as row_hash
    from customers
)
select * from new_columns