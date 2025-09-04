{% macro generate_dim_employee(audit_info, employee_source, territories_source, employeeter_source, region_source) %}

    WITH stg_employees AS (
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
            dl_process_date as employee_dl_processed_date
        FROM {{ employee_source }}
        
    ),
    stg_territories as (
        select
            territory_id,
            territory_description,
            region_id,
            dl_process_date as employee_territories_dl_processed_date
        from {{ territories_source }}
    ),
    stg_emplo_territories as (
        select
            territory_id,
            employee_id,
            dl_process_date as territories_dl_processed_date
        from {{ employeeter_source }}
    ),
    stg_region as (
        select
            region_id,
            region_description,
            dl_process_date as region_dl_processed_date
        from {{ region_source }}
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
            r.region_description,
            t.territory_description,
            greatest(
                e.employee_dl_processed_date,
                et.territories_dl_processed_date,
                t.employee_territories_dl_processed_date,
                r.region_dl_processed_date
            ) as max_dl_processed_date
        from stg_employees e
        inner join stg_emplo_territories et
            on et.employee_id = e.employee_id
        inner join stg_territories t
            on t.territory_id = et.territory_id
        inner join stg_region r
        on r.region_id = t.region_id
    ),
    current_view as (
        select
            *,
            -- Include all relevant fields in hash for change detection
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
            row_number() over(partition by employee_id order by max_dl_processed_date desc) as ranked   --- Data has duplicates (History)
        from joined
    ),
    source as (
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
            region_description,
            territory_description,
            row_hash,
            max_dl_processed_date as updated_at
        from current_view
        where ranked = 1 and max_dl_processed_date > TO_TIMESTAMP('{{ audit_info.hwm_date }}')  --- Filter max dl Data has duplicates (History)


        -- Dummy Records

        UNION ALL

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
            date('1900-01-01') as updated_at

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
            date('1900-01-01') as updated_at
    ),
    -- Get existing active records for comparison (only on incremental runs)
    existing_records AS (
        {% if is_incremental() %}
            SELECT 
                employee_scd_id,
                employee_id,
                first_name,
                last_name,
                title,
                title_of_courtesy,
                birthdate,
                address,
                city,
                postal_code,
                country,
                home_page,
                extension,
                region_description,
                territory_description,
                row_hash,
                updated_at,
                effective_date
            FROM {{ this }}
            WHERE is_active = TRUE
                AND employee_id IN (SELECT DISTINCT employee_id FROM source)
        {% else %}
        -- Return empty result set on first run
            SELECT 
                CAST(NULL AS STRING) as employee_scd_id,
                CAST(NULL AS INT) as employee_id,
                CAST(NULL AS STRING) as first_name,
                CAST(NULL AS STRING) as last_name,
                CAST(NULL AS STRING) as title,
                CAST(NULL AS STRING) as title_of_courtesy,
                CAST(NULL AS DATE) as birthdate,
                CAST(NULL AS STRING) as address,
                CAST(NULL AS STRING) as city,
                CAST(NULL AS STRING) as postal_code,
                CAST(NULL AS STRING) as country,
                CAST(NULL AS STRING) as home_page,
                CAST(NULL AS STRING) as extension,
                CAST(NULL AS STRING) as region_description,
                CAST(NULL AS STRING) as territory_description,
                CAST(NULL AS STRING) as row_hash,
                CAST(NULL AS TIMESTAMP) as updated_at,
                CAST(NULL AS TIMESTAMP) as effective_date
            WHERE FALSE
        {% endif %}
    ),
    scd_updates AS (
        SELECT
            s.employee_id,
            s.first_name,
            s.last_name,
            s.title,
            s.title_of_courtesy,
            s.birthdate,
            s.address,
            s.city,
            s.postal_code,
            s.country,
            s.home_page,
            s.extension,
            s.region_description,
            s.territory_description,
            s.row_hash,
            s.updated_at,
            -- Generate consistent surrogate key
            dateadd(day, 1,DATE('{{audit_info.last_processed_date}}')) as effective_date,
            {{ dbt_utils.generate_surrogate_key(['s.employee_id', 's.updated_at']) }} as employee_scd_id,
            CASE
                WHEN e.employee_scd_id IS NULL THEN 'N'  -- New record
                WHEN e.row_hash != s.row_hash THEN 'U'   -- Changed record  
                ELSE 'X'
            END AS change_type
        FROM source s
        LEFT JOIN existing_records e
            ON s.employee_id = e.employee_id
    ),
    -- New and updated records
    new_records AS (
        SELECT
            employee_scd_id,
            employee_id,
            first_name,
            last_name,
            title,
            title_of_courtesy,
            birthdate,
            address,
            city,
            postal_code,
            country,
            home_page,
            extension,
            region_description,
            territory_description,
            row_hash,
            updated_at,
            TRUE AS is_active,
            effective_date,
            NULL AS expiry_date
        FROM scd_updates
        WHERE change_type IN ('N', 'U')
    ),
    -- Records to expire (set as inactive) - only on incremental runs
    expired_records AS (
        {% if is_incremental() %}
            SELECT
                e.employee_scd_id,
                e.employee_id,
                e.first_name,
                e.last_name,
                e.title,
                e.title_of_courtesy,
                e.birthdate,
                e.address,
                e.city,
                e.postal_code,
                e.country,
                e.home_page,
                e.extension,
                e.region_description,
                e.territory_description,
                e.row_hash,
                e.updated_at,
                FALSE AS is_active,
                e.effective_date,
                s.effective_date AS expiry_date
            FROM existing_records e
            INNER JOIN scd_updates s
                ON e.employee_id = s.employee_id
            WHERE s.change_type = 'U'
        {% else %}
        -- Return empty result set on first run
            SELECT
                CAST(NULL AS STRING) as employee_scd_id,
                CAST(NULL AS INT) as employee_id,
                CAST(NULL AS STRING) as first_name,
                CAST(NULL AS STRING) as last_name,
                CAST(NULL AS STRING) as title,
                CAST(NULL AS STRING) as title_of_courtesy,
                CAST(NULL AS DATE) as birthdate,
                CAST(NULL AS STRING) as address,
                CAST(NULL AS STRING) as city,
                CAST(NULL AS STRING) as postal_code,
                CAST(NULL AS STRING) as country,
                CAST(NULL AS STRING) as home_page,
                CAST(NULL AS STRING) as extension,
                CAST(NULL AS STRING) as region_description,
                CAST(NULL AS STRING) as territory_description,
                CAST(NULL AS STRING) as row_hash,
                CAST(NULL AS TIMESTAMP) as updated_at,
                CAST(NULL AS BOOLEAN) as is_active,
                CAST(NULL AS TIMESTAMP) as effective_date,
                CAST(NULL AS TIMESTAMP) as expiry_date
            WHERE FALSE
        {% endif %}
    )
    -- Combine new/updated records with expired records
    SELECT * FROM new_records
    UNION ALL
    SELECT * FROM expired_records

    {% if is_incremental() %}
        UNION ALL
        -- Keep existing records that weren't affected
        SELECT 
            employee_scd_id,
            employee_id,
            first_name,
            last_name,
            title,
            title_of_courtesy,
            birthdate,
            address,
            city,
            postal_code,
            country,
            home_page,
            extension,
            region_description,
            territory_description,
            row_hash,
            updated_at,
            is_active,
            effective_date,
            expiry_date
        FROM {{ this }}
        WHERE employee_id NOT IN (SELECT DISTINCT employee_id FROM source)
    {% endif %}
    -- The end of the model
{% endmacro %}