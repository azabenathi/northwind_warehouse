{{ config(
    materialized='incremental',
    unique_key='customer_scd_id',
    on_schema_change='sync_all_columns',
    post_hook="{{ updating_dimension_audit('dim_customer') }}"
) }}

with source_data as (
    select 
        *,
        1 as version_no
    from {{ ref('stg_dim_customer') }}
),
existing_records as (
    {% if is_incremental() %}
        select
            customer_scd_id,
            customer_id,
            company_name,
            contact_name,
            contact_title,
            address,
            city,
            region,
            postal_code,
            country,
            phone,
            fax,
            version_no,
            row_hash,
            updated_at,
            effective_date
        from {{ this }}
        where is_active = 'Y'
            and customer_id in (select distinct customer_id from source_data)
    {% else %}
        select
            cast(null as string) as customer_scd_id,
            cast(null as string) as customer_id,
            cast(null as string) as company_name,
            cast(null as string) as contact_name,
            cast(null as string) as contact_title,
            cast(null as string) as address,
            cast(null as string) as city,
            cast(null as string) as region,
            cast(null as string) as postal_code,
            cast(null as string) as country,
            cast(null as string) as phone,
            cast(null as string) as fax,
            cast(null as int) as version_no,
            cast(null as string) as row_hash,
            cast(null as timestamp_ntz) as updated_at,
            cast(null as timestamp_ntz) as effective_date
        where false
    {% endif %}
),
scd_updates as (
    select
        c.customer_id,
        c.company_name,
        c.contact_name,
        c.contact_title,
        c.address,
        c.city,
        c.region,
        c.postal_code,
        c.country,
        c.phone,
        c.fax,
        c.op,
        c.version_no,
        c.row_hash,
        c.updated_at,
        c.effective_date,
        {{ dbt_utils.generate_surrogate_key(['c.customer_id', 'c.effective_date']) }} as customer_scd_id,
        case when e.customer_id is null then 'N' else 'U' end as change_type  
    from source_data c
    left outer join existing_records e
    on c.customer_id = e.customer_id

    union all

    select
        customer_scd_id,
        customer_id,
        company_name,
        contact_name,
        contact_title,
        address,
        city,
        region,
        postal_code,
        country,
        phone,
        'I' as op,
        fax,
        version_no,
        row_hash,
        updated_at,
        effective_date,
        'U' as change_type
    from existing_records
),
new_updates_records as (
    select
        customer_scd_id,
        customer_id,
        company_name,
        contact_name,
        contact_title,
        address,
        city,
        region,
        postal_code,
        country,
        phone,
        op,
        fax,
        change_type,
        version_no,
        row_hash,
        case 
            when op = 'D' then 'D'
            when coalesce(lag(row_hash,1) over (partition by employee_id order by effective_date),'X') <> row_hash 
                then 'U' 
            else 'X' 
        end as UPD_IND,
        updated_at,
        effective_date
    from scd_updates
    where change_type in ('N', 'U')
),
new_records as (
    select
        customer_scd_id,
        customer_id,
        company_name,
        contact_name,
        contact_title,
        address,
        city,
        region,
        postal_code,
        country,
        phone,
        fax,
        version_no,
        row_hash,
        updated_at,
        case 
            when UPD_IND = 'D' then 'D'
            when lead(effective_date,1) over (partition by employee_id order by effective_date) is null and UPD_IND <> 'D' then 'Y' 
            else 'N' 
        end as is_active,
        case
            when change_type = 'N' 
                then row_number() over (partition by employee_id order by effective_date) 
            when change_type = 'U' and row_number() over (partition by employee_id order by effective_date) > 1
                then (First_value(version_no) over (partition by employee_id order by effective_date) + row_number() over (partition by employee_id order by effective_date)) - 1  -- Calculate new version
            else
                version_no
        end as version_no,
        case 
            when row_number() over (partition by employee_id order by effective_date) = 1 and change_type IN ('N') 
                then cast('1900-01-01' as date) 
            else effective_date 
        end as effective_date,  -- set effective date to 1900-01-01 for very first occurrence of a natural key else use effective date
        coalesce(lead(effective_date,1) over (partition by employee_id order by effective_date), '3001-01-01'::timestamp_ntz) as expiry_date
    from new_updates_records
    where UPD_IND in ('U', 'D')
)
select * from new_records