{% macro fact_order_fail_lookup() %}
    {% set update_query %}
        update kings.dwh.fact_order
            set
                employee_sk = coalesce(de.employee_sk, {{ dbt_utils.generate_surrogate_key(["0", "to_timestamp_ntz('1900-01-01 00:00:00')"]) }}),
                customer_sk = coalesce(dc.customer_sk, {{ dbt_utils.generate_surrogate_key(["0", "to_timestamp_ntz('1900-01-01 00:00:00')"]) }}),
                shipper_sk = coalesce(ds.shipper_sk, {{ dbt_utils.generate_surrogate_key(["0", "to_timestamp_ntz('1900-01-01 00:00:00')"]) }}),
                updated_at = current_timestamp()
        from kings.dwh.fact_order fo
        left join {{ ref('dim_employee') }} de
            on de.employee_id = fo.employee_id and to_timestamp_ntz(fo.order_date) between de.effective_date and de.expiry_date
        left join {{ ref('dim_customer') }} dc
            on dc.customer_id = fo.customer_id and to_timestamp_ntz(fo.order_date) between dc.effective_date and dc.expiry_date
        left join {{ ref('dim_shipper') }} ds
            on ds.shipper_id = fo.shipper_id and to_timestamp_ntz(fo.order_date) between ds.effective_date and ds.expiry_date
        where fo.employee_sk = {{ dbt_utils.generate_surrogate_key(["0", "to_timestamp_ntz('1900-01-01 00:00:00')"]) }} 
            or fo.customer_sk = {{ dbt_utils.generate_surrogate_key(["0", "to_timestamp_ntz('1900-01-01 00:00:00')"]) }} 
            or fo.shipper_sk = {{ dbt_utils.generate_surrogate_key(["0", "to_timestamp_ntz('1900-01-01 00:00:00')"]) }}
    {% endset %}

    {% if execute %}
        {{ log('Failed lookups...', info=True) }}
        {% do run_query(update_query) %}
        {{ log('Updated lookup records on fact_order' , info=True) }}
    {% endif %}
{% endmacro %}