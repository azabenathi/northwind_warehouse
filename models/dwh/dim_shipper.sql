{{ config(
    materialized='incremental',
    unique_key='shipper_sk',
    on_schema_change='sync_all_columns',
    post_hook="{{ updating_audit('dim_customer') }}"
) }}

with source_data as (
    select 
        *,
        1 as version_no
    from {{ ref('stg_dim_shipper') }}
),
existing_records as (
    {% if is_incremental() %}
        select
            shipper_sk,
            shipper_id,
            company_name,
            phone,
            version_no,
            row_hash,
            updated_at,
            effective_date
        from {{ this }}
        where is_active = 'Y'
            and shipper_id in (select distinct shipper_id from source_data)
    {% else %}
        select
            cast(null as string) as shipper_sk,
            cast(null as int) as shipper_id,
            cast(null as string) as company_name,
            cast(null as string) as phone,
            cast(null as int) as version_no,
            cast(null as string) as row_hash,
            cast(null as timestamp_ntz) as updated_at,
            cast(null as timestamp_ntz) as effective_date
        where false
    {% endif %}
),
scd_updates as (
    select
        c.shipper_id,
        c.company_name,
        c.phone,
        c.op,
        c.row_hash,
        c.updated_at,
        c.version_no,
        c.effective_date,
        {{ dbt_utils.generate_surrogate_key(['c.shipper_id', 'c.effective_date']) }} as shipper_sk,
        case when e.shipper_id is null then 'N' else 'U' end as change_type  
    from source_data c
    left outer join existing_records e
    on e.shipper_id = c.shipper_id
    union all
    select
        e.shipper_id,
        e.company_name,
        e.phone,
        'I' as op,
        e.row_hash,
        e.updated_at,
        e.version_no,
        e.effective_date,
        e.shipper_sk,
        'U' as change_type
    from existing_records e
),
new_updates_records as (
    select
        shipper_sk,
        shipper_id,
        company_name,
        phone,
        version_no,
        change_type,
        row_hash,
        case 
            when op = 'D' then 'D'
            when coalesce(lag(row_hash,1) over (partition by shipper_id order by effective_date),'X') <> row_hash 
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
        shipper_sk,
        shipper_id,
        company_name,
        phone,
        row_hash,
        updated_at,
        case 
            when UPD_IND = 'D' then 'D'
            when lead(effective_date,1) over (partition by shipper_id order by effective_date) is null and UPD_IND <> 'D' then 'Y' 
            else 'N' 
        end as is_active,
        case
            when change_type = 'N' 
                then row_number() over (partition by shipper_id order by effective_date) 
            when change_type = 'U' and row_number() over (partition by shipper_id order by effective_date) > 1
                then (First_value(version_no) over (partition by shipper_id order by effective_date) + row_number() over (partition by shipper_id order by effective_date)) - 1  -- Calculate new version
            else
                version_no
        end as version_no,
        case 
            when row_number() over (partition by shipper_id order by effective_date) = 1 and change_type IN ('N') 
                then to_timestamp_ntz('1900-01-01 00:00:00')
            else effective_date 
        end as effective_date,  -- set effective date to 1900-01-01 for very first occurrence of a natural key else use effective date
        coalesce(lead(effective_date,1) over (partition by shipper_id order by effective_date), to_timestamp_ntz('3001-01-01 00:00:00')) as expiry_date
    from new_updates_records
    where UPD_IND in ('U', 'D')
)
select * from new_records