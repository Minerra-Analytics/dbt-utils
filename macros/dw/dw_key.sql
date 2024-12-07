{# To do: add adapter redirection to default__ #}

{#- ----------------------------------------------------------------------------------------------------------
  MACRO: dim_key
  DESCRIPTION:
    Generates a surrogate key for a dimension table. For single columns, returns the column value cast to the
    specified data type. For multiple columns or when hash=True, generates a hashed surrogate key using
    dbt_utils.generate_surrogate_key.

  PARAMETERS:
    col_name_list (string | list): Column name(s) to use for key generation
    key_name (string): Name of the output surrogate key column
    default (int, optional): Default value when source columns are null. Defaults to 0
    data_type (string, optional): Data type for the key column. Defaults to "bigint"
    hash (bool, optional): Force hash-based key generation. Defaults to False
    range_min (int, optional): Minimum value for key range. Defaults to 1
    range_max (int, optional): Maximum value for key range. Defaults to 999999999

  RETURNS:
    SQL expression that generates the surrogate key

  EXAMPLES:
    -- Single column key
    {{ dim_key("customer_id", "customer_key") }}

    -- Multi-column composite key
    {{ dim_key(["first_name", "last_name", "dob"], "customer_key") }}

    -- Forced hash key with custom defaults
    {{ dim_key("customer_id", "customer_key", default=999, data_type="integer", hash=True) }}
-#}
{%- macro dim_key(col_name_list, key_name, default=0, data_type="bigint", hash=False, range_min=1, range_max=999999999) -%}
{%- if not execute %}
    {{- return('') }}
{%- endif %}
{{- debug("col_name: " ~ col_name_list | join(", "), info=True) }}
{%- set pre = "cast(coalesce(" %}
{%- set post = ", " ~ default ~ ") as " ~ data_type ~ ")" %}
{%- if col_name_list is iterable and col_name_list is not string %}
{%-   set s_list = [] %}
{%-   for col_name in col_name_list %}
{{-     debug("col_name: " ~ col_name, info=True) }}
{%-     set s = pre ~ col_name ~ post %}
{{-     debug("s: [" ~ s ~ "]", info=True) }}
{%-     set _ = s_list.append(s) %}
{%-   endfor %}
{%- else %}
{%-   set s_list = [ pre ~ col_name_list ~ post ] %}
{%- endif %}
{{-   debug("s_list: [" ~ s_list | join("], [") ~ "]", info=True) }}
{%- if hash == True or ( col_name_list is not string and col_name_list is iterable and col_name_list | length > 1) %}
{{-   debug("hash keys: [" ~ col_name_list | join("], [") ~ "]", info=True) }}
{{-    dbt_utils.generate_surrogate_key(col_name_list) }} as {{ key_name }}
{%- else %}
{{-    s_list | join(" || ") }} as {{ key_name }}
{%- endif %}
{%- endmacro -%}



{#- ----------------------------------------------------------------------------------------------------------
  MACRO: fact_key
  DESCRIPTION:
    Generates a surrogate key for a fact table using dbt_utils.generate_surrogate_key. This is a simplified
    version of dim_key that always uses hashing and standard formatting.

  PARAMETERS:
    col_name_list (string | list): Column name(s) to use for key generation
    key_name (string): Name of the output surrogate key column

  RETURNS:
    SQL expression that generates the hashed surrogate key

  EXAMPLE:
    {{ fact_key(["order_date", "customer_key", "product_key"], "order_fact_key") }}
-#}

{%- macro fact_key(col_name_list, key_name) -%}
{%- if not execute %}
    {{- return('') }}
{%- endif %}
{%- set indent = " " * 12 %}
{{ indent }}{{    dbt_utils.generate_surrogate_key(col_name_list) }} as {{ key_name }}
{%- endmacro -%}


{#- ----------------------------------------------------------------------------------------------------------
  MACRO: dim_date_key
  DESCRIPTION:
    Generates a date-based surrogate key in YYYYMMDD format for date dimension tables.
    Converts the input date to an 8-digit integer representation.

  PARAMETERS:
    date_col (string): Name of the date column to convert
    key_name (string): Name of the output surrogate key column
    default_int (string, optional): Default value when date is null. Defaults to "0"

  RETURNS:
    SQL expression that generates the date-based surrogate key

  EXAMPLE:
    {{ dim_date_key("order_date", "date_key", default_int="19000101") }}
-#}
{% macro dim_date_key(date_col, key_name, default_int="0") -%}
{%- if not execute %}
    {{- return('') }}
{%- endif %}
{%- set indent = ' ' * 12 %}
{{ indent }}coalesce(TO_CHAR({{date_col}}::date, 'yyyymmdd')::INT,{{ default_int }}) as {{ key_name }}
{%- endmacro -%}
