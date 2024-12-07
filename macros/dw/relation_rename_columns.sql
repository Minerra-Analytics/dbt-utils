{#- ----------------------------------------------------------------------------------------------------------
 This macro generates SQL CTEs to help with column selection and renaming in dbt models.

 PARAMETERS
 ----------
 relation (Relation):
     The source table/view to select from. Can be a ref() or source().

 clean (bool, optional):
     When False, generates an additional CTE with a template for column renaming.
     Default: True

 except (list, optional):
     List of column names to exclude from selection.
     Default: None

 cte_name (str, optional):
     Name for the base CTE.
     Default: "base"

 RETURNS
 -------
 string
     SQL CTEs that include:
     1. A base CTE containing all columns from source
     2. When clean=False, a rename_template CTE for column renaming

 USAGE
 -----
 1. Generate template for renaming:
 ```sql
    with
    {{- dbt_utils.relation_rename_columns(ref('my_table'), clean=false) }}
```

 2. Exclude specific columns while generating template:
 ```sql
    with
    {{- dbt_utils.relation_rename_columns(
        ref('my_table'),
        clean=false,
        except=['excluded_col1', 'excluded_col2']
    ) }}
```

 3. Final implementation after renaming:
 ```sql
    with
    {{- dbt_utils.relation_rename_columns(ref('my_table')) }},
    renamed as (
        select
            id as customer_id,
            name as customer_name
        from base
    )
```

 WORKFLOW
 --------
 1. First run with clean=false to generate the rename template
 2. Copy the generated template CTE to your model
 3. Rename columns as needed in your model
 4. Set clean=true (or remove the parameter) in the macro call
#}
{%- macro relation_rename_columns(
    relation,
    clean=true,
    except=none,
    cte_name="base"
) %}
{{- debug("relation_rename_columns", info=true) }}
{{- debug("relation:" ~ relation) }}
{{- return(adapter.dispatch('relation_rename_columns', 'dbt_utils')(
    relation,
    clean=clean,
    except=except,
    cte_name=cte_name
))
}}
{%- endmacro %}

{%- macro default__relation_rename_columns(
    relation,
    clean=true,
    except=none,
    cte_name="base"
) %}

{%- set tabwidth = 4 %}
{%- set indent_level = 3 %}
{%- set indent = " " * indent_level * tabwidth %}

{#-
Indentation following sqlfmt:

with
#}
    {{ cte_name }} as (
        select
            -- Table: {{ base_name }} columns
{{-indent}}{{ dbt_utils.star(relation, indent=indent) }}
        from {{ relation }}
    )
{%- if clean %}
-- clean: {{clean}}: No template generated
{%- else -%}
    ,
    -- clean: {{clean}}: Template generated
    {{ cte_name }}_rename as (
        select
            {%- if except != none %}
            -- except: {{ except | join(", ") }}
            {%- endif %}
            -- Table: {{ base_name }} columns
{{-indent}}{{ dbt_utils.star(relation, indent=indent, except=except) }}
        from base
    )
{%- endif %}
{%- endmacro %}
