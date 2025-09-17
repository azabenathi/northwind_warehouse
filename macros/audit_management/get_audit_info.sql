{% macro get_audit_info_copy(dimension_name) %}
    {% if execute %}
        {% set audit_query %}
            SELECT 
                TO_TIMESTAMP_NTZ(hwm_date) as hwm_date,
                TO_TIMESTAMP_NTZ(last_processed_date) as last_processed_date,
                driver_table as driver_table,
                business_key as business_key,
                is_processed as is_processed,
                is_initialized as is_initialized
            FROM audit.audit_control
            WHERE entity = '{{ dimension_name }}'
            
        {% endset %}

    
        {% set results = run_query(audit_query) %}
        
        {% if results.rows %}
            {% set audit_info = {
                'hwm_date': results.columns[0].values()[0],
                'last_processed_date': results.columns[1].values()[0],
                'driver_table': results.columns[2].values()[0],
                'business_key': results.columns[3].values()[0],
                'is_processed': results.columns[4].values()[0],
                'is_initialized': results.columns[5].values()[0]
            } %}
        {% else %}
            {% set audit_info = {
                'hwm_date': modules.datetime.datetime(1900,1,1,0,0,0),
                'last_processed_date': modules.datetime.datetime(1900,1,1,0,0,0),
                'driver_table': '',
                'business_key': '',
                'is_processed': false,
                'is_initialized': false
            } %}
        {% endif %}
        {{ return(audit_info) }}
    {% else %}

        {{ return({
            'hwm_date': modules.datetime.datetime(1900,1,1,0,0,0),
            'last_processed_date': modules.datetime.datetime(1900,1,1,0,0,0),
            'driver_table': '',
            'business_key': '',
            'is_processed': false,
            'is_initialized': false
        }) }}
        
    {% endif %}
{% endmacro %}