{{
    config(
        pre_hook="{{ initialized_audit('dim_products', 'stg_products', 'product_id') }}"
    )
}}

{% set dimension_name = 'dim_products' %}
{% set audit_info = get_audit_info(dimension_name) %}

{{ log('audit_info: '~audit_info, info=True)}}

-- Loop Depends on this
{% set products_source = ref('stg_products') %}
{% set suppliers_source = ref('stg_suppliers') %}
{% set categories_source = ref('stg_categories')  %}

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

    {{ log('last_processed: ' ~ last_processed, info=True)}}
    {{ log('current_date: ' ~ current_date, info=True)}}
    {{ log('minutes_behind: ' ~ minutes_behind, info=True)}}
    
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
                        WITH stg_products AS (
                            -- Driver Table
                            SELECT
                                product_id,
                                product_name,
                                supplier_id,
                                category_id,
                                quantity_per_unit,
                                unit_price,
                                units_in_stock,
                                units_on_order,
                                reorder_level,
                                discontinued,
                                dl_process_date,
                                op
                            FROM {{ products_source }}
                            AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz)                    
                        ),
                        stg_suppliers as (
                            select
                                supplier_id,
                                company_name,
                                address,
                                city,
                                region,
                                postal_code,
                                country,
                                dl_process_date,
                                op
                            from {{ suppliers_source }}
                            AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz)
                            where op <> 'D'    
                        ),
                        stg_categories as (
                            select
                                category_id,
                                category_name,
                                description,
                                dl_process_date,
                                op
                            from {{ categories_source }}
                            AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz)
                            where op <> 'D'        
                        ),
                        joined as (
                        select
                            p.product_id,
                            p.product_name,
                            p.quantity_per_unit,
                            p.unit_price,
                            p.reorder_level,
                            p.discontinued,
                            p.op,
                            s.company_name,
                            s.address,
                            s.city,
                            s.region,
                            s.postal_code,
                            s.country,
                            c.category_name,
                            c.description,
                            greatest(
                                p.dl_process_date,
                                s.dl_process_date,
                                c.dl_process_date
                            ) as max_dl_processed_date
                        from stg_products p
                        inner join stg_suppliers s
                            on p.supplier_id = s.supplier_id
                        inner join stg_categories c
                        on p.category_id = c.category_id
                    ),
                    current_view as (
                        select
                            *,
                            -- Include all relevant fields in hash for change detection     /   taking care of the null columns as well: coalesce
                            {{ dbt_utils.generate_surrogate_key([
                                    'product_id',
                                    'product_name',
                                    'quantity_per_unit',
                                    'reorder_level',
                                    'discontinued',
                                    'company_name',
                                    'address',
                                    'city',
                                    'region',
                                    'postal_code',
                                    'country',
                                    'category_name',
                                    'description',
                            ]) }} as row_hash,
                            row_number() over(partition by product_id order by max_dl_processed_date desc) as ranked   --- Not required: Data has duplicates (History)
                        from joined
                    )
                    select 
                        product_id,
                        product_name,
                        quantity_per_unit,
                        unit_price,
                        reorder_level,
                        discontinued,
                        company_name,
                        address,
                        city,
                        region,
                        postal_code,
                        country,
                        category_name,
                        description,
                        op,
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
                        p.dl_process_date,
                        s.dl_process_date,
                        c.dl_process_date
                    ) as max_dl_processed_date 
                    FROM {{ products_source }} AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz) as p
                        INNER JOIN {{ suppliers_source }} AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz) as s
                            ON p.supplier_id = s.supplier_id
                        INNER JOIN {{ categories_source }} AT (TIMESTAMP => '{{ time_travel }}'::timestamp_ntz) as c
                            ON p.category_id = c.category_id
                    WHERE greatest(
                        p.dl_process_date,
                        s.dl_process_date,
                        c.dl_process_date
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
            0 as product_id,
            'Not Found' as product_name,
            'Not Found' as quantity_per_unit,
            0           as unit_price,
            0           as reorder_level,
            TRUE        as discontinued,
            'Not Found' as company_name,
            'Not Found' as address,
            'Not Found' as city,
            'Not Found' as region,
            'Not Found' as postal_code,
            'Not Found' as country,
            'Not Found' as category_name,
            'Not Found' as description,
            'I' as op,
            {{ dbt_utils.generate_surrogate_key([
               'product_id',
                'product_name',
                'quantity_per_unit',
                'unit_price',
                'reorder_level',
                'discontinued',
                'company_name',
                'address',
                'city',
                'region',
                'postal_code',
                'country',
                'category_name',
                'description',
            ]) }} as row_hash,
            to_timestamp_ntz('1900-01-01 10:00:00') as updated_at,
            to_timestamp_ntz('1900-01-01 10:00:00') as effective_date

        union all

        select
            -1 as product_id,
            'Not Applicable' as product_name,
            'Not Applicable' as quantity_per_unit,
            -1               as unit_price,
            -1               as reorder_level,
            TRUE             as discontinued,
            'Not Applicable' as company_name,
            'Not Applicable' as address,
            'Not Applicable' as city,
            'Not Applicable' as region,
            'Not Applicable' as postal_code,
            'Not Applicable' as country,
            'Not Applicable' as category_name,
            'Not Applicable' as description,
            'I' as op,
            {{ dbt_utils.generate_surrogate_key([
               'product_id',
                'product_name',
                'quantity_per_unit',
                'unit_price',
                'reorder_level',
                'discontinued',
                'company_name',
                'address',
                'city',
                'region',
                'postal_code',
                'country',
                'category_name',
                'description',
            ]) }} as row_hash,
            to_timestamp_ntz('1900-01-01 10:00:00') as updated_at,
            to_timestamp_ntz('1900-01-01 10:00:00') as effective_date
    ),
    -- Last part | meeting
    ranked as (
        select 
            *,
            row_number() over(partition by product_id, updated_at order by updated_at) as rn   -- Need to check this: Not required   
        from final_source
    ) 
    select 
        * 
    from ranked 
    where rn = 1 
    
{% endif %}