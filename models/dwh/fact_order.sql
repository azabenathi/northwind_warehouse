{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='sync_all_columns',
        pre_hook="{{ initialize_dimension_audit('fact_order', 'stg_orders', 'order_id') }}",
        post_hook="{{ updating_dimension_audit('fact_order') }}"     
    )
}}

{% set dimension_name = 'fact_order' %}
{% set audit_info = get_dimension_audit_info(dimension_name) %}

select
    fo.order_id,
    coalesce(de.employee_scd_id, md5('0' || '-' || '1900-01-01 10:00:00')) as employee_scd_id,
    coalesce(dc.customer_scd_id, md5('0' || '-' || '1900-01-01 10:00:00')) as customer_scd_id,
    coalesce(ds.shipper_scd_id, md5('0' || '-' || '1900-01-01 10:00:00')) as shipper_scd_id,
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
    fo.ship_postal_code,
    fo.ship_country,
    fo.dl_process_date as updated_at,
    fo.row_hash
from {{ ref('stg_orders') }} fo
left join {{ ref('dim_employee') }} de
    on de.employee_id = fo.employee_id and to_timestamp_ntz(fo.order_date) between de.effective_date and de.expiry_date
left join {{ ref('dim_customer') }} dc
    on dc.customer_id = fo.customer_id and to_timestamp_ntz(fo.order_date) between dc.effective_date and dc.expiry_date
left join {{ ref('dim_shipper') }} ds
    on ds.shipper_id = fo.shipper_id and to_timestamp_ntz(fo.order_date) between ds.effective_date and ds.expiry_date

{% if is_incremental() %}
    where fo.dl_process_date > TO_TIMESTAMP_NTZ('{{ audit_info.hwm_date }}')
{% endif %}