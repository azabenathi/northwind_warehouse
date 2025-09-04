{% macro get_missing_dates(target_table, date_column) %}
  
  {% set query %}
    with date_spine as (
      select dateadd('day', seq4(), current_date() - 7) as check_date
      from table(generator(rowcount => 7))
    ),
    existing_dates as (
      select distinct date_trunc('day', {{ date_column }}) as existing_date
      from {{ target_table }}
      where {{ date_column }} >= current_date() - 7
    )
    select check_date
    from date_spine
    left join existing_dates on date_spine.check_date = existing_dates.existing_date
    where existing_dates.existing_date is null
  {% endset %}
  
  {% set results = run_query(query) %}
  {% if execute %}
    {% set missing_dates = results.columns[0].values() %}
    {{ return(missing_dates) }}
  {% endif %}
  
{% endmacro %}