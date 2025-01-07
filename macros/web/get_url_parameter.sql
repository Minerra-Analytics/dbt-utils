{% macro get_url_parameter(field, url_parameter) -%}
    {{ return(adapter.dispatch('get_url_parameter', 'dwa')(field, url_parameter)) }}
{% endmacro %}

{% macro default__get_url_parameter(field, url_parameter) -%}

{%- set formatted_url_parameter = "'" + url_parameter + "='" -%}

{%- set split = dbt.split_part(dbt.split_part(field, formatted_url_parameter, 2), "'&'", 1) -%}

nullif({{ split }},'')

{%- endmacro %}
