{#- ----------------------------------------------------------------------------------------------------------
  This macro generates a SELECT statement that casts unknown values to specified data types for each column in a given table.
   It takes the following parameters:
   - relation: A relation object.
   - unknown_numeric: The value to cast as unknown for numeric data types. Default is 0.
   - unknown_text: The value to cast as unknown for text data types. Default is "Unassigned".
   - unknown_date: The value to cast as unknown for date data types. Default is "9999-01-01".

   Example usage:
   {{ unknown("my_table", unknown_numeric=999, unknown_text="Unknown", unknown_date="2022-01-01") }}
#}
{%- macro unknown(relation, unknown_numeric=var("unknown_number"), unknown_text=var("unknown_string"), unknown_date=var("unknown_date"), id_col_list=[]) %}
{%- set all_columns = get_columns_in_relation(relation) %}
  select
    {%- for col in all_columns %}
    {#- debug("col: " ~ col.name ~ " [" ~ col.data_type ~ "]", info=True) #}
    CAST(
    {%- if col.data_type in ["text","character varying","varchar"] -%}
    {{ unknown_text }}
    {% elif col.data_type in ["integer","smalint","bigint","double precision","decimal","numeric","real","double precision","small serial","serial","bigserial","boolean"] -%}
    {{ unknown_numeric }}
    {% elif col.data_type in ["date","timestamp with time zone","timestamp without time zone"] -%}
    {{ unknown_date }}
    {% elif col.data_type in ["time with time zone","time without time zone"] -%}
    '00:00:00'
    {%- else -%}
    NULL
    {%- endif -%}
     AS {{ col.data_type }}) AS "{{ col.name | lower -}}"
    {%- if not loop.last %}, {% endif %}
    {%- endfor %}
{%- endmacro %}
