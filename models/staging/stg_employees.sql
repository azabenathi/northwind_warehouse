with employees as (
    select * from {{ source("northwind","employees") }}
),
new_colums as (
    select
        {{dbt_utils.generate_surrogate_key(["EmployeeID"])}} as hash_key,
        EmployeeID as employee_id,
        LastName as last_name,
        FirstName as first_name,
        Title as title,
        TitleOfCourtesy as title_of_courtesy,
        BirthDate as birth_date,
        Address as address,
        City as city,
        Region as region,
        PostalCode as postal_code,
        Country as country,
        HomePhone as home_page,
        Extension as extension,
        Photo as photo,
        Notes as notes,
        ReportsTo as reports_to,
        PhotoPath as photo_path,
        current_timestamp() as dl_process_date,
        'I' as op,
        {{ dbt_utils.generate_surrogate_key([
            "EmployeeID",
            "LastName",
            "FirstName",
            "Title",
            "TitleOfCourtesy",
            "BirthDate",
            "Address",
            "City",
            "Region",
            "PostalCode",
            "Country",
            "HomePhone",
            "Extension",
            "Photo",
            "Notes",
            "ReportsTo",
            "PhotoPath"
        ]) }} as record_hash
    from employees
)
select * from new_colums