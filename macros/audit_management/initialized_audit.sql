{% macro initialize_dimension_audit(dimension_name, driver_table, business_key) %}
    
    {% set create_table %}
        
        CREATE TABLE IF NOT EXISTS kings.audit.audit_control (
            dimension_name string primary key,
            hwm_date timestamp_ntz default '1900-01-01 10:00:00'::timestamp_ntz,
            driver_table string,
            business_key string,
            is_processed boolean default false,
            is_initialized boolean default false,
            last_processed_date timestamp_ntz default '1900-01-01 10:00:00'::timestamp_ntz,
            created_at timestamp_ntz default cast(current_timestamp as timestamp_ntz)
        );
    {% endset %}

    {% set insert_query %}
        insert into kings.audit.audit_control (dimension_name, driver_table, business_key, is_initialized)
        select '{{dimension_name}}','{{driver_table}}', '{{business_key}}', true
        where not exists (
            select 1
            from kings.audit.audit_control
            where dimension_name = '{{ dimension_name }}'
        );
    {% endset%}
    
    {% if execute %}
        {{ log('creating... ', info=True) }}
        {% do run_query(create_table) %}
        {{ log('Table created ' ~ dimension_name, info=True) }}
        
        {% do run_query(insert_query) %}
        {{ log('Initialized audit entry for ' ~ dimension_name, info=True) }}
    {% endif %}
    
{% endmacro %}