
{#- ----------------------------------------------------------------------------------------------------------
    This macro performs a lookup operation between two tables based on a specified column.
    It returns only one value of the return column and renames it as specified.
    
    Parameters:
        - main_table: The name of the main table to perform the lookup on.
        - col_to_lookup: The column in the main table to use for the lookup.
        - lookup_table: The name of the lookup table.
        - lookup_col: The column in the lookup table to match with the main table's column.
        - return_col: The column in the lookup table to return as the result of the lookup.
        - rename_col: The name to assign to the returned column in the main table.
    
    Returns:
        A SELECT statement that joins the main table with the lookup table and includes the specified columns.
#}
{%- macro lookup_one(main_table, col_to_lookup, lookup_table, lookup_col=col_to_lookup, return_col=lookup_col, rename_col=return_col, lookup_default="0", return_default="NULL", picker_fn="min") %}
  {%- if not execute %}
        {{ return('') }}
  {% endif -%}
    with lookup_table as (
        SELECT
            {{lookup_col}},
            {%- if picker_fn == "first" %}{{return_col}},
            row_number() over (partition by {{lookup_col}} order by {{return_col}}) = 1 as is_dedupe
            {%- elif picker_fn == "string_agg"%}{{picker_fn}}({{return_col}}::text, ',') AS {{return_col}}
            {%- else %}{{picker_fn}}({{return_col}}) AS {{return_col}}
            {%- endif %}
        FROM {{lookup_table}}
        {% if picker_fn != "first" %}group by {{lookup_col}}{% endif %}
    )
    SELECT
        main_table.*,
        COALESCE(lookup_table.{{return_col}}, {{return_default}}) AS {% if return_col == lookup_col %} _new_{{rename_col}}{%- else %} {{rename_col}}{%- endif %}
    FROM
        {{main_table}} AS main_table
    LEFT JOIN lookup_table
        {%- if lookup_default %}
        ON coalesce(main_table.{{col_to_lookup}}, {{lookup_default}}) = lookup_table.{{lookup_col}}
        {%- else %}
        ON main_table.{{col_to_lookup}} = lookup_table.{{lookup_col}}
        {%- endif %}
    {% if picker_fn == "first" %}where lookup_table.is_dedupe{% endif %}
{% endmacro %}


{%- macro lookup_dim_key(main_table, col_to_lookup, lookup_table, lookup_col=col_to_lookup, return_col=lookup_col, rename_col=return_col, lookup_default="0", return_default="NULL", picker_fn="min") %}
{{- debug("lookup_dim_key--------") }}
{%- set same_name_prefix = "_new_" %}
{#- Set default lookup_col to be same as col_to_lookup for convenience #}
{%- if not lookup_col %}{%- set lookup_col = col_to_lookup %}{%- endif %}
{#- Set default rename_col to be same as return_col for convenience #}
{%- if not rename_col %}{%- set rename_col = return_col %}{%- endif %}
{{- 
  debug(
    "main_table: " ~ main_table ~ " " ~
    "\ncol_to_lookup: " ~ col_to_lookup ~ " " ~  
    "\nlookup_table: " ~ lookup_table ~ " " ~
    "\nlookup_col: " ~ lookup_col ~ " " ~
    "\nreturn_col: " ~ return_col ~ " " ~
    "\nrename_col: " ~ rename_col ~ " " ~
    "\nreturn_default: " ~ return_default
    ,info=True) 
}}
  /*
  This query performs a lookup of {{col_to_lookup}} in "{{main_table}}" table or CTE from the "{{lookup_table}}" table and returning the minimum value of "{{return_col}}" for each of {{lookup_col}}.
  {%- if lookup_col != return_col %}The "{{return_col}}" column is renamed to "{{same_name_prefix}}{{rename_col}}"{%- endif %}.
  If there is no matching record in the "{{lookup_table}}" table, the "{{return_col}}" column is set to "{{return_default}}".
  */
{#- We can accept a column name string or a list of column name strings #}
{#-  col_to_lookup, lookup_col #}
{%- if col_to_lookup is not string and col_to_lookup is iterable %}
{{- debug("col_to_lookup is a list", info=True) }}
{%- set cols_to_lookup = col_to_lookup %}
{%- set lookup_cols = lookup_col %}
{%- else %}
{{- debug("col_to_lookup is a string", info=True) }}
{%- set cols_to_lookup = [ col_to_lookup ] %}
{%- set lookup_cols = [ lookup_col ] %}
{%- endif %}
{#-  return_col, rename_col, return_default #}
{%- if return_col is not string and return_col is iterable %}
{{- debug("return_col is a list", info=True) }}
{%- set return_col_list = return_col %}
{%- set rename_col_list = rename_col %}
{%- set return_default_list = return_default %}
{%- else %}
{{- debug("return_col is a string", info=True) }}
{%- set return_col_list = [ return_col ] %}
{%- set rename_col_list = [ rename_col ] %}
{%- set return_default_list = [ return_default ] %}
{%- endif %}
  SELECT
    main_table.*,
{%- for return_col, rename_col, return_default in zip(return_col_list, rename_col_list, return_default_list) %}
    COALESCE(lookup_table.{% if return_col == lookup_col -%}{{same_name_prefix}}{%- endif -%}{{rename_col}}, {{return_default}}) AS {{rename_col}}
{%-   if not loop.last %}, {% endif %}
{%- endfor %}
  FROM
    {{main_table}} AS main_table
  LEFT JOIN
  (
    SELECT
{%- for lookup_col in lookup_cols %}
      {{lookup_col}}
{%-   if not loop.last %}, {% endif %}
{%- endfor %},
{%- if picker_fn == "first" %}
        row_number() over ( partition by 
{%-   for lookup_col in lookup_cols %} 
        {{-" "}}{{lookup_col}}
{%-     if not loop.last %},{%endif%}
{%-   endfor %} 
        {{-" "}}order by 
{%-   for return_col in return_col_list %} 
        {{-" "}}{{return_col}}
{%-     if not loop.last %},{%endif%}
{%-   endfor -%}
        ) = 1 as is_dedupe,
{%-   for  return_col, rename_col in zip(return_col_list, rename_col_list) %}
        {{return_col}} AS {% if return_col in lookup_cols %}{{same_name_prefix}}{%- endif %}{{rename_col}}
{%-     if not loop.last %}, {% endif %}
{%-   endfor %}
{%- else %}
{%-   for  return_col, rename_col in zip(return_col_list, rename_col_list) %}
        {{picker_fn}}({{return_col}}) AS {% if return_col in lookup_cols %}{{same_name_prefix}}{%- endif %}{{rename_col}}
{%-     if not loop.last %}, {% endif %}
{%-   endfor %}
{%- endif %}
    FROM
      {{lookup_table}}
{%- if picker_fn == "first" %}
{%- else %}
    GROUP BY
{%-   for lookup_col in lookup_cols %}
      {{lookup_col}}
{%-     if not loop.last %}, {% endif %}
{%-   endfor %}
{%- endif %}
  ) AS lookup_table
    ON 
{%- for col_to_lookup, lookup_col, default in zip(cols_to_lookup, lookup_cols, lookup_default) %}
    COALESCE(main_table.{{col_to_lookup}}, {{default}}) = lookup_table.{{lookup_col}} 
    {# main_table.{{col_to_lookup}} = lookup_table.{{lookup_col}} #}
{%-   if not loop.last %} AND {% endif %}
{%- endfor %}
{%- if picker_fn == "first" %}
    where is_dedupe
{%- else %}
{%- endif %}
{%- endmacro %}

{%- macro lookup_return_many_cols(main_table, col_to_lookup, lookup_table, lookup_col, return_col, rename_col, lookup_default="0", return_default="NULL", picker_fn="min") %}
{{- lookup_dim_key(main_table=main_table, col_to_lookup=col_to_lookup, lookup_table=lookup_table, lookup_col=lookup_col, return_col=return_col, rename_col=rename_col, lookup_default=lookup_default, return_default=return_default, picker_fn=picker_fn) }}
{%- endmacro %}
