{{ config(
    materialized='incremental',
    unique_key='product_sk',
    on_schema_change='sync_all_columns',
    post_hook="{{ updating_dimension_audit('dim_products') }}"
) }}

with source_data as (
    select 
        *,
        1 as version_no
    from {{ ref('stg_dim_products') }}
),
existing_records AS (
    {% if is_incremental() %}
        SELECT 
            product_sk,
            product_id,
            product_name,
            quantity_per_unit,
            unit_price,
            reorder_level,
            discontinued,
            company_name,
            address,
            city,
            region,
            postal_code,
            country,
            category_name,
            description,
            row_hash,
            version_no,
            updated_at,
            effective_date,
            expiry_date
        FROM {{ this }}
        WHERE is_active = 'Y'
            AND product_id IN (SELECT DISTINCT product_id FROM source_data)
    {% else %}
        -- Return empty result set on first run
        SELECT 
            CAST(NULL AS STRING)    as product_sk,
            CAST(NULL AS INT)       as product_id,
            CAST(NULL AS STRING)    as product_name,
            CAST(NULL AS STRING)    as quantity_per_unit,
            CAST(NULL AS INT)       as unit_price,
            CAST(NULL AS INT)       as reorder_level,
            CAST(NULL AS STRING)   as discontinued,
            CAST(NULL AS STRING)    as company_name,
            CAST(NULL AS STRING)    as address,
            CAST(NULL AS STRING)    as city,
            CAST(NULL AS STRING)    as region,
            CAST(NULL AS STRING)    as postal_code,
            CAST(NULL AS STRING)    as country,
            CAST(NULL AS STRING)    as category_name,
            CAST(NULL AS STRING)    as description,
            CAST(NULL AS STRING)    as row_hash,
            CAST(NULL AS INT)       as version_no,
            CAST(NULL AS TIMESTAMP_NTZ) as updated_at,
            CAST(NULL AS TIMESTAMP_NTZ) as effective_date,
            CAST(NULL AS TIMESTAMP_NTZ) as expiry_date
        WHERE FALSE
    {% endif %}
),
scd_updates AS (
    SELECT
        s.product_id,
        s.product_name,
        s.quantity_per_unit,
        s.unit_price,
        s.reorder_level,
        s.discontinued,
        s.company_name,
        s.address,
        s.city,
        s.region,
        s.postal_code,
        s.country,
        s.category_name,
        s.description,
        s.op,
        s.row_hash,
        s.updated_at,
        s.version_no,
        -- Generate consistent surrogate key
        s.effective_date,
        {{ dbt_utils.generate_surrogate_key(['s.product_id', 's.effective_date']) }} as product_sk,
        case when e.product_id is null then 'N' else 'U' end as change_type  
    FROM source_data s
    left outer join existing_records e
    on e.product_id = s.product_id
    union all   
    SELECT
        s.product_id,
        s.product_name,
        s.quantity_per_unit,
        s.unit_price,
        s.reorder_level,
        s.discontinued,
        s.company_name,
        s.address,
        s.city,
        s.region,
        s.postal_code,
        s.country,
        s.category_name,
        s.description,
        'I' as op,
        s.row_hash,
        s.updated_at,
        s.version_no,
        s.effective_date,
        s.product_sk,
        'U' as change_type
    FROM existing_records s
),
new_update_records AS (
    SELECT
        product_sk,
        product_id,
        product_name,
        quantity_per_unit,
        unit_price,
        reorder_level,
        discontinued,
        company_name,
        address,
        city,
        region,
        postal_code,
        country,
        category_name,
        description,
        change_type,
        version_no,
        row_hash,
        case 
            when op = 'D' then 'D'
            when coalesce(lag(row_hash,1) over (partition by product_id order by effective_date),'X') <> row_hash 
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
        product_sk,
        product_id,
        product_name,
        quantity_per_unit,
        unit_price,
        reorder_level,
        discontinued,
        company_name,
        address,
        city,
        region,
        postal_code,
        country,
        category_name,
        description,
        row_hash,
        updated_at,
        case 
            when UPD_IND = 'D' then 'D'
            when lead(effective_date,1) over (partition by product_id order by effective_date) is null and UPD_IND <> 'D' then 'Y' 
            else 'N' 
        end as is_active,
        case
            when change_type = 'N' 
                then row_number() over (partition by product_id order by effective_date) 
            when change_type = 'U' and row_number() over (partition by product_id order by effective_date) > 1
                then (First_value(version_no) over (partition by product_id order by effective_date) + row_number() over (partition by product_id order by effective_date)) - 1  -- Calculate new version
            else
                version_no
        end as version_no,
        case 
            when row_number() over (partition by product_id order by effective_date) = 1 and change_type IN ('N') 
                then cast('1900-01-01' as date) 
            else effective_date 
        end as effective_date,  -- set effective date to 1900-01-01 for very first occurrence of a natural key else use effective date
        coalesce(lead(effective_date,1) over (partition by product_id order by effective_date), '3001-01-01'::timestamp_ntz) as expiry_date
    FROM new_update_records
    where UPD_IND in ('U', 'D') --- filter on actual new or changed rows and deleted
)

select 
    *
from new_records

