{% macro nullcheck_table(relation) %}
    {{ return(adapter.dispatch('nullcheck_table', 'dwa')(relation)) }}
{% endmacro %}

{% macro default__nullcheck_table(relation) %}

  {%- do dwa._is_relation(relation, 'nullcheck_table') -%}
  {%- do dwa._is_ephemeral(relation, 'nullcheck_table') -%}
  {% set cols = adapter.get_columns_in_relation(relation) %}

  select {{ dwa.nullcheck(cols) }}
  from {{relation}}

{% endmacro %}
