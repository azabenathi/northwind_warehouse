{{
    config(
        materialized='incremental',
        unique_key = 'order_id',
        on_schema_change = 'sync_all_columns',
        pre_hook = "{{ initialized_audit('fact_order_details', 'stg_order_details', 'order_id') }}",
        post_hook = " {{ updating_audit('fact_order_details') }}"
    )
}}

{% set dimension_name = 'fact_order_details' %}
{% set audit_info = get_audit_info(dimension_name) %}

select 
    od.order_id,
    od.product_id,
    od.unit_price,
    od.quantity,
    od.discount,
    od.dl_process_date as updated_at,
    od.op,
    dp.product_sk,
    dp.product_name,
    dp.quantity_per_unit,
    dp.reorder_level,
    dp.discontinued,
    dp.company_name,
    dp.address,
    dp.city,
    dp.region,
    dp.postal_code,
    dp.country,
    dp.category_name,
    dp.description,
    dp.version_no,
    dp.effective_date,
    od.row_hash
from {{ ref('stg_order_details' )}} od 
left join {{ ref('dim_products' )}} dp
    on dp.product_id = od.product_id and od.dl_process_date between dp.effective_date and dp.expiry_date

{% if is_incremental() %}
    where od.updated_at > TO_TIMESTAMP_NTZ('{{ audit_info.hwm_date }}')
{% endif %}