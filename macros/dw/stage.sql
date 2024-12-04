{#- ----------------------------------------------------------------------------------------------------------
 Macro: stage_sql
  
 This macro generates a SQL query to select all columns from a specified stage table.
  
 Parameters:
  - stage_name: The name of the stage table.
  - except: A list of columns to exclude from the selection.
  
 Returns:
  A SQL query that selects all columns from the specified stage table, excluding the columns specified in the 'except' parameter.
#}
{%- macro stage_sql(stage_name, except=[]) -%}
  {{ debug("stage_sql() - execute:" ~ execute, info=True) }}
  select 
    {{ dbt_utils.star(stage_name, except=except) }}
  from {{ stage_name }}
{%- endmacro %}

