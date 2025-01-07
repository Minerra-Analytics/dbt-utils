{%- macro surrogate_key(field_list) -%}
    {% set frustrating_jinja_feature = varargs %}
    {{ return(adapter.dispatch('surrogate_key', 'dwa')(field_list, *varargs)) }}
{% endmacro %}

{%- macro default__surrogate_key(field_list) -%}

{%- set error_message = '
Warning: `dwa.surrogate_key` has been replaced by \
`dwa.generate_surrogate_key`. The new macro treats null values \
differently to empty strings. To restore the behaviour of the original \
macro, add a global variable in dbt_project.yml called \
`surrogate_key_treat_nulls_as_empty_strings` to your \
dbt_project.yml file with a value of True. \
The {}.{} model triggered this warning. \
'.format(model.package_name, model.name) -%}

{%- do exceptions.raise_compiler_error(error_message) -%}

{%- endmacro -%}
