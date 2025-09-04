{% macro post_hook_dimension_audit(dimension_name) %}
    {% if execute %}
        {% set date_query %}
            select max(updated_at) from {{ this }}
        {% endset %}
        
        {% set date_results = run_query(date_query) %}
        {% set hwm_date = date_results.columns[0].values()[0] %}
        
        {% set last_date = modules.datetime.datetime.now() %}  -- The final answer
        
        {# Check something #}
        {#
                {% if (last_date | string) == '1900-01-01' or (last_date >= modules.datetime.date.today()) %}
                    {% set last_date = modules.datetime.date.today().strftime('%Y-%m-%d') %}
                {% else %}
                    {% set current_date = modules.datetime.date.today() %}
                    {% set days_behind = (current_date - audit_info.last_processed_date).days %}
                    {% set last_date = audit_info.last_processed_date + modules.datetime.timedelta(days=1) %}
                {% endif %}
        #}


        {{ updating_dimension_audit(dimension_name, hwm_date, last_date) }}
    {% endif %}
{% endmacro %}