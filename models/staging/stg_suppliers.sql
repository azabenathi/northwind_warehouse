with suppliers as (
    select * from {{ source("northwind","suppliers") }}
),
new_columns as (
    select
        supplierid as supplier_id,
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
        homepage as home_page,
        current_timestamp() as dl_process_date,
        'I' as op,
        {{ dbt_utils.generate_surrogate_key([
            "supplier_id",
            "company_name",
            "contact_name",
            "contact_title",
            "address",
            "city",
            "region",
            "postal_code",
            "country"
        ]) }} as row_hash
    from suppliers
)
select * from new_columns