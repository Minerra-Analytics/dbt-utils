{% macro get_tables_by_prefix_sql(schema, prefix, exclude='', database=target.database) %}
    {{ return(adapter.dispatch('get_tables_by_prefix_sql', 'dwaema, prefix, exclude, database)) }}
{% endmacro %}

{% macro default__get_tables_by_prefix_sql(schema, prefix, exclude='', database=target.database) %}

    {{ dwaables_by_pattern_sql(
        schema_pattern = schema,
        table_pattern = prefix ~ '%',
        exclude = exclude,
        database = database
    ) }}

{% endmacro %}
