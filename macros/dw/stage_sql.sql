
{#- ----------------------------------------------------------------------------------------------------------
 Macro: stage_sql

 This macro generates a SQL query to select all columns from a specified stage table.

 Parameters:
  - relation: A relation object.
  - except: A list of columns to exclude from the selection.

 Returns:
  A SQL query that selects all columns from the specified stage table, excluding the columns specified in the 'except' parameter.
#}
{%- macro stage_sql(relation, except=[]) -%}
{%- if not execute %}
    {{- return('') }}
{%- endif %}

{{- debug("stage_sql() - execute:" ~ execute, info=True) }}
{%- set indent = ' ' * 8 %}
{{ indent }}select
                {{- dbt_utils.star(relation, except=except, indent=' ' * 12) }}
{{ indent }}from {{ relation }}
{%- endmacro %}
