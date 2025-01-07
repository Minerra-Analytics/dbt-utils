{% macro log_info(message) %}
    {{ return(adapter.dispatch('log_info', 'dwasage)) }}
{% endmacro %}

{% macro default__log_info(message) %}
    {{ log(dway_log_format(message), info=True) }}
{% endmacro %}
