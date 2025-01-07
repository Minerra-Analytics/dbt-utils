{% macro my_custom_macro() %}
    whatever
{% endmacro %}

{% macro limit_zero() %}
    {{ return(adapter.dispatch('limit_zero', 'dwa')()) }}
{% endmacro %}

{% macro default__limit_zero() %}
    {{ return('limit 0') }}
{% endmacro %}
