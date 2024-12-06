{#- ----------------------------------------------------------------------------------------------------------
 This macro generates a common table expression (CTE) for constructing a stage table from a base table.

 Parameters:
  - base_name (Relation): The source table/view to select from. Can be a ref() or source().
  - clean (bool, optional): When True, skips the rename_template CTE. Default: True.
  - except (list, optional): List of column names to exclude from selection. Default: None.

 Returns:
  string: A SQL CTE that includes:
    1. A 'base' CTE containing all columns from source
    2. When clean=False, a 'rename_template' CTE for column renaming

 Example Usage:
    -- Include all columns, generate renaming template
    {{ dbt_utils.base_cte(ref('my_table'), clean=False) }}

    In vs code and using dbt power user, compile this and copy the rename_template cte to the main editor.
    Rename the columns using "column_a" as "useful_column"
    Set the clean=True or remove the clean argument (Defaults to True).

    -- Exclude columns and generate rename template
    {{ dbt_utils.base_cte(
        base_name=ref('my_table'),
        clean=False,
        except=['excluded_col1', 'excluded_col2']
    ) }}

 Description:
 This macro constructs a stage table from a base table by performing the following steps:
  - Creates a base CTE with all columns from the source table
  - When clean=False (default), adds a rename template CTE for column renaming
  - Returns the resulting CTE for constructing the stage table
#}
{%- macro base_cte(
    base_name,
    clean=true,
    except=none
) %}
{{ log("base_cte", info=true) }}
{{ return(adapter.dispatch('base_cte', 'dbt_utils')(
    base_name,
    clean,
    except
))
}}
{%- endmacro %}

{% macro default__base_cte(
    base_name,
    clean=true,
    except=none
) %}

{%- set tabwidth = 4 %}
{%- set indent_level = 3 %}
{%- set indent = " " * indent_level * tabwidth %}

with
    base as (
        select
            {%- if except != none %}
            -- except: {{ except | join(", ") }}
            {%- endif %}
            -- Table: {{ base_name }} columns
{{-indent}}{{ dbt_utils.star(base_name, indent=indent) }}
        from {{ base_name }}
    )
{%- if not clean %}
    ,
    /* Copy and paste the above columns into main editor and rename the columns to naming convention */
    rename as (
        select
{{-indent}}{{ dbt_utils.star(base_name, indent=indent, except=except) }}
        from base
    )
    select *
    from rename
{%- endif %}
{%- endmacro %}
