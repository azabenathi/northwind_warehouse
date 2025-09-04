{% snapshot snapshot_employee %}

    {{
        config(
            target_schema='SNAPSHOTS',
            unique_key='employee_scd_id',
            strategy='timestamp',
            updated_at='updated_at'
        )
    }}


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
    FROM {{ ref('stg_employees') }}
),

stg_territories as (
    select
        territory_id,
        territory_description,
        region_id,
        dl_process_date as employee_territories_dl_processed_date
    from {{ ref('stg_territories') }}
),

stg_emplo_territories as (
    select
        territory_id,
        employee_id,
        dl_process_date as territories_dl_processed_date
    from {{ ref('stg_employeeterritories') }}
),

stg_region as (
    select
        region_id,
        region_description,
        dl_process_date as region_dl_processed_date
    from {{ ref('stg_region') }}
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
        row_number() over(partition by employee_id order by max_dl_processed_date desc) as ranked
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
        TO_TIMESTAMP(max_dl_processed_date) as updated_at
    from current_view
    where ranked = 1
)
select 
    md5(concat_ws('||',cast('employee_id' as string), updated_at)) as employee_scd_id,
    * from source
{% endsnapshot %}