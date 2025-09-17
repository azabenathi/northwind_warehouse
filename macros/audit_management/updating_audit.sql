{% macro updating_audit(dimension_name) %}

    {% set last_processed_date = modules.datetime.datetime.now() %} 
    {% set init_query %}
        UPDATE kings.audit.audit_control
        SET 
            -- hwm_date = '{{ hwm_date }}',
            hwm_date = (select max(updated_at) from {{ this }}),
            is_processed = true,
            is_initialized = true,
            last_processed_date = '{{ last_processed_date }}'
        WHERE dimension_name = '{{ dimension_name }}';
    {% endset %}
    
    {% if execute %}
        {{ log('Updating...', info=True) }}
        {% do run_query(init_query) %}
        {{ log('Updating entry for ' ~ dimension_name, info=True) }}
    {% endif %}
    
{% endmacro %}