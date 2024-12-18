
{#- ----------------------------------------------------------------------------------------------------------
  This macro, named "dw_incremental", is used to construct the incremental logic for the incremental table.
  Parameters:
  - updated_date_col: The name of the updated date column. Default is "etl_updated_date".

  It performs the following steps:
  - insert the standard incremental logic for the incremental table (updated_date_col and unique_key must be present in the table)

  Returns:
  - Nothing

  Usage:
  - inserted at the last line of the model file or CTE after the last select statement

  Example:
  with base as (
    select *
    from {{ ref('my_table') }}
  ),
  rename as (
    select
    ...
    from base
  )
  select * from rename
  {{ dw_incremental("updated_date") }}
#}
{% macro dw_incremental(updated_date_col) %}
{{- debug("dw_incremental") }}
{%- if not execute %}
{{-   return("") }}
{%- endif %}
{{- debug("updated_date_col: " ~ updated_date_col) }}
{%- if is_incremental() and execute %}
{%-   set col_list = dbt_utils.get_filtered_columns_in_relation( this ) %}
{%-   if updated_date_col in col_list %}
    where {{updated_date_col}} > (select coalesce(max({{updated_date_col}}), '{{var("min_timestamp")}}') from {{ this }})
{%-   else %}
{{-     exceptions.raise_compiler_error("dw_incremental: Invalid call as `updated_date_col` does not exist. Got `updated_date_col`:" ~ updated_date_col ~ "\n  column list:" ~ col_list) }}
{%-   endif %}
{%- endif %}
{% endmacro %}
