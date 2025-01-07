{% macro pretty_log_format(message) %}
    {{ return(adapter.dispatch('pretty_log_format', 'dwa')(message)) }}
{% endmacro %}

{% macro default__pretty_log_format(message) %}
    {{ return( dwa.pretty_time() ~ ' + ' ~ message) }}
{% endmacro %}
