{#- ----------------------------------------------------------------------------------------------------------
    This macro generates NULL values cast to the appropriate data type for all columns in a specified relation.
    It's particularly useful when you need to create a structure-only copy of a table or when you need
    placeholder NULL values with the correct data types.

    Parameters:
        - from: The source relation (table/view or CTE) to base the column structure on
        - relation_alias: Alias for the relation (default: False)
        - except: List of column names to exclude from the result (default: [])
        - prefix: String to prepend to each column name (default: '')
        - suffix: String to append to each column name (default: '')
        - quote_identifiers: Whether to quote column identifiers (default: True)
        - indent: Indentation to use for the generated SQL (default: '')

    Returns:
        A SQL string that includes NULL casts for each column in the source relation, maintaining the original
        data types. During parsing mode, returns '*'. If no columns are found, returns a commented star during
        compilation or a comment during runtime.

    Example:
        select {{ dbt_utils.star_null(
            from=ref('my_table'),
            except=['id', 'created_at'],
            prefix='new_'
        ) }}
#}
{% macro star_null(from, relation_alias=False, except=[], prefix='', suffix='', quote_identifiers=True, indent='') -%}
    {%- do dbt_utils._is_relation(from, 'star') -%}
    {%- do dbt_utils._is_ephemeral(from, 'star') -%}

    {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {% do return('*') %}
    {%- endif -%}

    {%- set include_cols = [] %}
    {%- set include_type = [] %}
    {%- set cols = adapter.get_columns_in_relation(from) -%}
    {%- set except = except | map("lower") | list %}
    {%- for col in cols -%}
        {%- if col.column|lower not in except -%}
            {% do include_cols.append(col) %}
        {%- endif %}
    {%- endfor %}

    {%- set cols = include_cols %}

    {%- if cols|length <= 0 -%}
        {% if flags.WHICH == 'compile' %}
            {% set response %}
*
/* No columns were returned. Maybe the relation doesn't exist yet
or all columns were excluded. This star is only output during
dbt compile, and exists to keep SQLFluff happy. */
            {% endset %}
            {% do return(response) %}
        {% else %}
            {% do return("/* no columns returned from star() macro */") %}
        {% endif %}
    {%- else -%}
        {%- for col in cols -%}
            cast(NULL as {{col.data_type}}){{" as "}}
                {%- if quote_identifiers -%}
                    {%- if prefix!='' or suffix!='' %} {{- (prefix ~ col.quoted ~ suffix)|trim }} {%- else %} {{- col.quoted|trim }} {%- endif -%}
                {%- else -%}
                    {%- if prefix!='' or suffix!='' %} {{- (prefix ~ col.name ~ suffix)|trim }} {%- else %} {{- col.name|trim }} {%- endif -%}
                {%- endif %}
            {%- if not loop.last %},{{ '\n  ' }}{%- endif -%}
        {%- endfor -%}
    {%- endif %}
{%- endmacro %}
