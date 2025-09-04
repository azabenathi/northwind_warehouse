{{
    config(
        pre_hook="{{ initialize_dimension_audit('dim_employee', 'stg_employees', 'employee_id') }}"
    )
}}

{% set dimension_name = 'dim_employee' %}
{% set audit_info = get_dimension_audit_info(dimension_name) %}

{{ log('audit_info: '~audit_info, info=True)}}

-- Loop Depends on this
{% set employee_source = ref('stg_employees') %}
{% set territories_source = ref('stg_territories') %}
{% set employeeter_source = ref('stg_employeeterritories')  %}
{% set region_source = ref('stg_region') %}

{% if execute %}
    {% set last_processed = audit_info.last_processed_date %}
    --  First time it runs
    {% set state = {'hwm_date': audit_info.hwm_date} %}
    {% set current_date = modules.datetime.datetime.now() %}

    {{ log('before-check-last_processed: '~last_processed, info=True)}}

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

    {{ log('last_processed: '~last_processed, info=True)}}
    {{ log('current_date: '~current_date, info=True)}}
    {{ log('minutes_behind: '~minutes_behind, info=True)}}
    
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

            {% if not loop.first %} union all {% endif %}   -- Union all after the first iteration
            (
                        WITH stg_employees AS (
                            -- Driver Table
                            SELECT
                                employee_id,
                                first_name,
                                last_name,
                                title,
                                title_of_courtesy,
                                date(birth_date) as birthdate,
                                address,
                                city,
                                region,
                                postal_code,
                                country,
                                home_page,
                                extension,
                                op,
                                dl_process_date as employee_dl_processed_date
                            FROM {{ employee_source }}
                            AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz)                    
                        ),
                        stg_territories as (
                            select
                                territory_id,
                                territory_description,
                                region_id,
                                dl_process_date as employee_territories_dl_processed_date
                            from {{ territories_source }}
                            AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz)
                            where op <> 'D'    
                        ),
                        stg_emplo_territories as (
                            select
                                territory_id,
                                employee_id,
                                dl_process_date as territories_dl_processed_date
                            from {{ employeeter_source }}
                            AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz)
                            where op <> 'D'        
                        ),
                        stg_region as (
                            select
                                region_id,
                                region_description,
                                dl_process_date as region_dl_processed_date
                            from {{ region_source }}
                            AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz)
                            where op <> 'D'       
                        ),
                        joined as (
                        select
                            e.employee_id,
                            e.first_name,
                            e.last_name,
                            e.title,
                            e.title_of_courtesy,
                            e.birthdate,
                            e.address,
                            e.city,
                            e.region,
                            e.postal_code,
                            e.country,
                            e.home_page,
                            e.extension,
                            e.op,
                            r.region_description,
                            t.territory_description,
                            greatest(
                                e.employee_dl_processed_date,
                                et.territories_dl_processed_date,
                                t.employee_territories_dl_processed_date,
                                r.region_dl_processed_date
                            ) as max_dl_processed_date
                        from stg_employees e
                        -- inner join stg_emplo_territories et
                        left join stg_emplo_territories et
                            on et.employee_id = e.employee_id
                        inner join stg_territories t
                            on t.territory_id = et.territory_id
                        inner join stg_region r
                        on r.region_id = t.region_id
                    ),
                    current_view as (
                        select
                            *,
                            -- Include all relevant fields in hash for change detection     /   taking care of the null columns as well: coalesce
                            {{ dbt_utils.generate_surrogate_key([
                                'employee_id',
                                'first_name',
                                'last_name',
                                'title', 
                                'title_of_courtesy',
                                'birthdate',
                                'address',
                                'city',
                                'region',
                                'postal_code',
                                'country',
                                'home_page',
                                'extension',
                                'region_description',
                                'territory_description'
                            ]) }} as row_hash,
                            row_number() over(partition by employee_id order by max_dl_processed_date desc) as ranked   --- Not required: Data has duplicates (History)
                        from joined
                    )
                    select 
                        employee_id,
                        first_name,
                        last_name,
                        title,
                        title_of_courtesy,
                        birthdate,
                        address,
                        city,
                        region,
                        postal_code,
                        country,
                        home_page,
                        extension,
                        op,
                        region_description,
                        territory_description,
                        row_hash,
                        max_dl_processed_date as updated_at
                        -- To be removed: First time run of the Dim
                    ,   {% if (audit_info.last_processed_date | string) == '1900-01-01 10:00:00' -%}
                            TO_TIMESTAMP_NTZ('{{ audit_info.last_processed_date }}')
                        {%- else -%}
                            TO_TIMESTAMP_NTZ('{{ time_travel }}')
                        {%- endif -%} as effective_date
                    from current_view
                    where ranked = 1 and max_dl_processed_date > TO_TIMESTAMP_NTZ('{{ state.hwm_date }}') 
                    -- and max_dl_processed_date <= TO_TIMESTAMP_NTZ('{{  time_travel  }}')
            -- The end of the model
                    
            )

            -- Updating the high watermark for the loop
        {% set max_processed_query %}
                SELECT coalesce(max(max_dl_processed_date),TO_TIMESTAMP_NTZ('{{ state.hwm_date }}'))
                FROM (
                    SELECT greatest(
                        e.dl_process_date,
                        et.dl_process_date,
                        t.dl_process_date,
                        r.dl_process_date
                    ) as max_dl_processed_date 
                    FROM {{ employee_source }} AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz) as e
                        INNER JOIN {{ employeeter_source }} AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz) as et
                            ON et.employee_id = e.employee_id
                        INNER JOIN {{ territories_source }} AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz) as t
                            ON t.territory_id = et.territory_id  
                        INNER JOIN {{ region_source }} AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz) as r
                            ON r.region_id = t.region_id
                    WHERE greatest(
                        e.dl_process_date,
                        et.dl_process_date,
                        t.dl_process_date,
                        r.dl_process_date
                    ) > TO_TIMESTAMP_NTZ('{{ state.hwm_date }}')
                ) subq
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
            0 as employee_id,
            'Not Found' as first_name,
            'Not Found' as last_name,
            'Not Found' as title,
            'Not Found' as title_of_courtesy,
            date('1900-01-01') as birthdate,
            'Not Found' as address,
            'Not Found' as city,
            'Not Found' as region,
            'Not Found' as postal_code,
            'Not Found' as country,
            'Not Found' as home_page,
            'Not Found' as extension,
            'I' as op,
            'Not Found' as region_description,
            'Not Found' as territory_description,
            {{ dbt_utils.generate_surrogate_key([
                'employee_id',
                'first_name',
                'last_name',
                'title', 
                'title_of_courtesy',
                'birthdate',
                'address',
                'city',
                'region',
                'postal_code',
                'country',
                'home_page',
                'extension',
                'region_description',
                'territory_description'
            ]) }} as row_hash,
            to_timestamp_ntz('1900-01-01 10:00:00') as updated_at,
            to_timestamp_ntz('1900-01-01 10:00:00') as effective_date

        union all

        select
            -1 as employee_id,
            'Not Applicable' as first_name,
            'Not Applicable' as last_name,
            'Not Applicable' as title,
            'Not Applicable' as title_of_courtesy,
            date('1900-01-01') as birthdate,
            'Not Applicable' as address,
            'Not Applicable' as city,
            'Not Applicable' as region,
            'Not Applicable' as postal_code,
            'Not Applicable' as country,
            'Not Applicable' as home_page,
            'Not Applicable' as extension,
            'I' as op,
            'Not Applicable' as region_description,
            'Not Applicable' as territory_description,
            {{ dbt_utils.generate_surrogate_key([
                'employee_id',
                'first_name',
                'last_name',
                'title', 
                'title_of_courtesy',
                'birthdate',
                'address',
                'city',
                'region',
                'postal_code',
                'country',
                'home_page',
                'extension',
                'region_description',
                'territory_description'
            ]) }} as row_hash,
            to_timestamp_ntz('1900-01-01 10:00:00') as updated_at,
            to_timestamp_ntz('1900-01-01 10:00:00') as effective_date
    ),
    -- Last part | meeting
    ranked as (
        select 
            *,
            row_number() over(partition by employee_id, updated_at order by updated_at) as rn   -- Need to check this: Not required   
        from final_source
    ) 
    select 
        * 
    from ranked 
    where rn = 1 
    
{% endif %}