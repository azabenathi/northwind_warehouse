with employees as (
    select * from {{ source("northwind","employees") }}
),
new_colums as (
    select
        EmployeeID as employee_id,
        LastName as last_name,
        FirstName as first_name,
        title,
        TitleOfCourtesy as title_of_courtesy,
        BirthDate as birth_date,
        address,
        city,
        region,
        PostalCode as postal_code,
        country,
        HomePhone as home_page,
        extension,
        photo,
        notes,
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
            "Extension"
        ]) }} as row_hash
    from employees
)
select * from new_colums