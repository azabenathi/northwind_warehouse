{% macro get_processing_date_range(dimension_name, target_date=none) %}
    
    {% set target_date = target_date or (modules.datetime.date.today() - modules.datetime.timedelta(days=1)) %}  -- Check this
    {% set audit_info = get_dimension_audit_info(dimension_name) %}   
    {% set start_date = audit_info.last_processed_date or target_date %}
    {% set needs_processing = start_date <= target_date %}

    {{ return({
        'start_date': start_date,
        'end_date': target_date,
        'hwm_date': audit_info.hwm_date,
        'is_processed': audit_info.is_processed or false,
        'driver_table': audit_info.driver_table,
        'business_key': audit_info.business_key,
        'needs_processing': needs_processing,
        'total_days': (target_date - start_date).days + 1 if needs_processing else 0
    }) }}
{% endmacro %}