{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='sync_all_columns',
        pre_hook="{{ initialized_audit('fact_order', 'stg_orders', 'order_id') }}",
        post_hook=["{{ fact_order_fail_lookup() }}", "{{ updating_audit('fact_order') }}"]   
    )
}}

{% set dimension_name = 'fact_order' %}
{% set audit_info = get_audit_info(dimension_name) %}

with fct_source as (
    select
        fo.order_id,
        coalesce(de.employee_sk, {{ dbt_utils.generate_surrogate_key(["0", "to_timestamp_ntz('1900-01-01 00:00:00')"]) }}) as employee_sk,
        coalesce(dc.customer_sk, {{ dbt_utils.generate_surrogate_key(["0", "to_timestamp_ntz('1900-01-01 00:00:00')"]) }}) as customer_sk,
        coalesce(ds.shipper_sk, {{ dbt_utils.generate_surrogate_key(["0", "to_timestamp_ntz('1900-01-01 00:00:00')"]) }}) as shipper_sk,
        fo.employee_id,
        fo.customer_id,
        fo.shipper_id,
        fo.order_date,
        fo.required_date,
        fo.shipped_date,
        fo.freight,
        fo.shipname,
        fo.ship_address,
        fo.ship_city,
        fo.ship_region,
        fo.op as record_status,
        fo.ship_postal_code,
        fo.ship_country,
        fo.dl_process_date,
        fo.row_hash
    from {{ ref('stg_orders') }} fo
    left join {{ ref('dim_employee') }} de
        on de.employee_id = fo.employee_id and to_timestamp_ntz(fo.order_date) between de.effective_date and de.expiry_date
    left join {{ ref('dim_customer') }} dc
        on dc.customer_id = fo.customer_id and to_timestamp_ntz(fo.order_date) between dc.effective_date and dc.expiry_date
    left join {{ ref('dim_shipper') }} ds
        on ds.shipper_id = fo.shipper_id and to_timestamp_ntz(fo.order_date) between ds.effective_date and ds.expiry_date
    where TO_TIMESTAMP_NTZ(fo.dl_process_date) > TO_TIMESTAMP_NTZ('{{ audit_info.hwm_date }}')
),
existing_fact as (
    {% if is_incremental() %}
        select
            order_id,
            employee_sk,
            customer_sk,
            shipper_sk,
            employee_id,
            customer_id,
            shipper_id,
            order_date,
            required_date,
            shipped_date,
            freight,
            shipname,
            ship_address,
            ship_city,
            ship_region,
            ship_postal_code,
            ship_country,
            row_hash,
            record_status,
            dl_process_date,
            created_at,
            updated_at
        from {{ this }}
        where order_id in (select distinct order_id from fct_source)
    {% else %}
        select
            cast(null as int) as order_id,
            cast(null as string) as employee_sk,
            cast(null as string) as customer_sk,
            cast(null as string) as shipper_sk,
            cast(null as int) as employee_id,
            cast(null as string) as customer_id,
            cast(null as int) as shipper_id,
            cast(null as date) as order_date,
            cast(null as date) as required_date,
            cast(null as date) as shipped_date,
            cast(null as float) as freight,
            cast(null as string) as shipname,
            cast(null as string) as ship_address,
            cast(null as string) as ship_city,
            cast(null as string) as ship_region,
            cast(null as string) as ship_postal_code,
            cast(null as string) as ship_country,
            cast(null as string) as row_hash,
            cast(null as string) as record_status,
            CAST(NULL AS TIMESTAMP_NTZ) as dl_process_date,
            CAST(NULL AS TIMESTAMP_NTZ) as updated_at,
            CAST(NULL AS TIMESTAMP_NTZ) as created_at
        where false
    {% endif %}
),
final as (
    select
        fs.order_id,
        fs.employee_sk,
        fs.customer_sk,
        fs.shipper_sk,
        fs.employee_id,
        fs.customer_id,
        fs.shipper_id,
        fs.order_date,
        fs.required_date,
        fs.shipped_date,
        fs.freight,
        fs.shipname,
        fs.ship_address,
        fs.ship_city,
        fs.ship_region,
        fs.ship_postal_code,
        fs.ship_country,
        fs.dl_process_date,
        fs.row_hash,
        fs.record_status,
        case
            when ef.order_id is null
                then current_timestamp()
            else
                created_at
        end as created_at,
        current_timestamp() as updated_at
    from fct_source fs
    left join existing_fact ef
        on ef.order_id = fs.order_id
    where ef.order_id is null 
        or (ef.order_id is not null and ef.row_hash <> fs.row_hash)
        or (ef.order_id is not null and ef.record_status <> 'D' and fs.record_status = 'D')
)

select * from final
