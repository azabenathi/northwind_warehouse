{{ config(
    materialized='incremental',
    unique_key='employee_sk',
    on_schema_change='sync_all_columns',
    post_hook="{{ updating_dimension_audit('dim_employee') }}"
) }}

with source_data as (
    select 
        *,
        1 as version_no
    from {{ ref('stg_dim_employee') }}
),
existing_records AS (
    {% if is_incremental() %}
        SELECT 
            employee_sk,        -- change to employee_sk
            employee_id,
            first_name,
            last_name,
            title,
            title_of_courtesy,
            birthdate,
            address,
            city,
            postal_code,
            country,
            home_page,
            extension,
            region_description,
            territory_description,
            version_no,
            row_hash,
            updated_at,
            effective_date
        FROM {{ this }}
        WHERE is_active = 'Y'
            AND employee_id IN (SELECT DISTINCT employee_id FROM source_data)
    {% else %}
        -- Return empty result set on first run
        SELECT 
            CAST(NULL AS STRING) as employee_sk,
            CAST(NULL AS INT) as employee_id,
            CAST(NULL AS STRING) as first_name,
            CAST(NULL AS STRING) as last_name,
            CAST(NULL AS STRING) as title,
            CAST(NULL AS STRING) as title_of_courtesy,
            CAST(NULL AS DATE) as birthdate,
            CAST(NULL AS STRING) as address,
            CAST(NULL AS STRING) as city,
            CAST(NULL AS STRING) as postal_code,
            CAST(NULL AS STRING) as country,
            CAST(NULL AS STRING) as home_page,
            CAST(NULL AS STRING) as extension,
            CAST(NULL AS INT) as version_no,
            CAST(NULL AS STRING) as region_description,
            CAST(NULL AS STRING) as territory_description,
            CAST(NULL AS STRING) as row_hash,
            CAST(NULL AS TIMESTAMP_NTZ) as updated_at,
            CAST(NULL AS TIMESTAMP_NTZ) as effective_date
        WHERE FALSE
    {% endif %}
),
scd_updates AS (
    SELECT
        s.employee_id,
        s.first_name,
        s.last_name,
        s.title,
        s.title_of_courtesy,
        s.birthdate,
        s.address,
        s.city,
        s.postal_code,
        s.country,
        s.home_page,
        s.extension,
        s.region_description,
        s.territory_description,
        s.op,
        s.row_hash,
        s.updated_at,
        s.version_no,
        -- Generate consistent surrogate key
        s.effective_date,
        {{ dbt_utils.generate_surrogate_key(['s.employee_id', 's.effective_date']) }} as employee_sk,
        case when e.employee_id is null then 'N' else 'U' end as change_type  
    FROM source_data s
    left outer join existing_records e
    on e.employee_id = s.employee_id
    union all   
    SELECT
        s.employee_id,
        s.first_name,
        s.last_name,
        s.title,
        s.title_of_courtesy,
        s.birthdate,
        s.address,
        s.city,
        s.postal_code,
        s.country,
        s.home_page,
        s.extension,
        s.region_description,
        s.territory_description,
        'I' as op,
        s.row_hash,
        s.updated_at,
        s.version_no,
        s.effective_date,
        s.employee_sk,
        'U' as change_type
    FROM existing_records s
),
new_update_records AS (
    SELECT
        employee_sk,
        employee_id,
        first_name,
        last_name,
        title,
        title_of_courtesy,
        birthdate,
        address,
        city,
        postal_code,
        country,
        home_page,
        extension,
        region_description,
        territory_description,
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
    FROM scd_updates
    WHERE change_type IN ('N', 'U') 
),   
new_records AS (
    SELECT
        employee_sk,
        employee_id,
        first_name,
        last_name,
        title,
        title_of_courtesy,
        birthdate,
        address,
        city,
        postal_code,
        country,
        home_page,
        extension,
        region_description,
        territory_description,
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
    FROM new_update_records
    where UPD_IND in ('U', 'D') --- filter on actual new or changed rows and deleted
)

select * from new_records

