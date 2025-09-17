{% macro generate_audit_table() %}
    
    {% set create_table %}
        
        CREATE TABLE IF NOT EXISTS audit.audit_control (
            entity string primary key,
            hwm_date timestamp_ntz default '1900-01-01 00:00:00'::timestamp_ntz,
            driver_table string,
            business_key string,
            is_processed boolean default false,
            is_initialized boolean default false,
            last_processed_date timestamp_ntz default '1900-01-01 00:00:00'::timestamp_ntz,
            created_at timestamp_ntz default cast(current_timestamp as timestamp_ntz)
        );
    {% endset %}

    {% if execute %}

        {{ log('creating... ', info=True) }}
        {% do run_query(create_table) %}
        {{ log('Table created ', info=True) }}

    {% endif %}

{% endmacro %}