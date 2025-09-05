{{
    config(
        pre_hook="{{ initialize_dimension_audit('dim_shippers', 'stg_shippers', 'shipper_id') }}"
    )
}}

{% set dimension_name = 'dim_shippers' %}
{% set audit_info = get_dimension_audit_info(dimension_name) %}

{% set shipper_source = ref('stg_shippers') %}

{% if execute %}
    {% set last_processed = audit_info.last_processed_date %}
    --  First time it runs
    {% set state = {'hwm_date': audit_info.hwm_date} %}
    {% set current_date = modules.datetime.datetime.now() %}

    {#   {% if (last_processed.date() | string) == '1900-01-01' or (last_processed.date() >= modules.datetime.date.today()) %}  #} 
    {% if (last_processed.date() | string) == '1900-01-01' or (last_processed >= modules.datetime.datetime.now()) %}
       {# {% set last_processed = modules.datetime.date.today() - modules.datetime.timedelta(days=1)%} #}   -- actual date: going back a day
        {% set last_processed = modules.datetime.datetime.now() - modules.datetime.timedelta(minutes=1) %}    -- actual timestamp: going back a minute: For testing
    {#    {% set days_behind = 1 %}     #}
        {% set minutes_behind = 1%}
    {% else %}
    {#  {% set last_processed = last_processed.date()%}
        {% set current_date = modules.datetime.date.today() %}
        
        {% set days_behind = (current_date - last_processed).days %}    #}
    --   For testing
        {% set minutes_behind = ((current_date - last_processed).total_seconds() / 60) | int %}
    {% endif %}

    {% set count = 1 %}

    with final_source as (
        {% for v in range(minutes_behind) %}
            -- {{ log('Count: '~loop.index, info=True) }}
        {#  {% set time_travel = modules.datetime.datetime(last_processed.year, last_processed.month, last_processed.day, 7,10,0) + modules.datetime.timedelta(days=loop.index) %}  #}
            {% set time_travel = (last_processed + modules.datetime.timedelta(minutes=loop.index)) - modules.datetime.timedelta(hours=2) %}
            {{ log('Timetravel: '~time_travel, info=True) }}
            {{ log('Current_Timestamp: '~modules.datetime.datetime.now(), info=True) }}

            -- Reset Time travel
            {% if time_travel >= modules.datetime.datetime.now() %}
                {% set time_travel = modules.datetime.datetime.now()%}
            {% endif %}

            {% if not loop.first %} union all {% endif %}  
            (                
                select
                    shipper_id,
                    company_name,
                    phone,
                    op,
                    row_hash,
                    dl_process_date as updated_at,
                    {% if (audit_info.last_processed_date | string) == '1900-01-01 10:00:00' -%}
                        TO_TIMESTAMP_NTZ('{{ audit_info.last_processed_date }}')
                    {%- else -%}
                        TO_TIMESTAMP_NTZ('{{ time_travel }}')
                    {%- endif -%} as effective_date
                from {{ shipper_source }}
                AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz)
                where dl_process_date > TO_TIMESTAMP_NTZ('{{ state.hwm_date }}')
                
            )

            {% set max_processed_query %}
                SELECT coalesce(max(dl_process_date),TO_TIMESTAMP_NTZ('{{ state.hwm_date }}'))
                FROM {{ shipper_source }} AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz)
                WHERE dl_process_date > TO_TIMESTAMP_NTZ('{{ state.hwm_date }}')
            {% endset %}
            {% set results = run_query(max_processed_query) %}
            {% if results %}
                {% set _ = state.update({ 'hwm_date': results.columns[0].values()[0]}) %}
                {{ log('inside-hwm_date: '~state.hwm_date, info=True) }}
            {% endif%}        -- To be tested very well     

        {% endfor %}

        UNION ALL

        -- Union Dummy records
        select
            0 as shipper_id,
            'Not Found' as company_name,
            'Not Found' as phone,
            'I' as op,
            {{ dbt_utils.generate_surrogate_key([
                'shipper_id',
                'company_name',
                'phone',
            ]) }} as row_hash,
            to_timestamp_ntz('1900-01-01 10:00:00') as updated_at,
            to_timestamp_ntz('1900-01-01 10:00:00') as effective_date

        union all

        select
            -1 as shipper_id,
            'Not Applicable' as company_name,
            'Not Applicable' as phone,
            'I' as op,
            {{ dbt_utils.generate_surrogate_key([
                'shipper_id',
                'company_name',
                'phone'
            ]) }} as row_hash,
            to_timestamp_ntz('1900-01-01 10:00:00') as updated_at,
            to_timestamp_ntz('1900-01-01 10:00:00') as effective_date
    ),
    ranked as (
        select
            *,
             row_number() over(partition by shipper_id, updated_at order by updated_at) as rn
        from final_source
    )
    select 
        * 
    from ranked 
    where rn = 1 
{% endif %}