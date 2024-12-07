# Data Warehouse Macros

## lookup.sql

### Simple left join to a lookup table

- Lookup using left join on lookup columns
- Main table has a unique key column with no nulls
- Where return vaue is null, a default value is provided

Expected behaviour:
- Number of rows does not change and remain unique
- Columns returned are added to the main table using the rename column names
- The default values is used where it is specified
- The returned column names are renamed with the given names

Performance considerations:
- The join columns in the lookup table should be indexed for optimal performance
- Indexing the join columns in the main table is not necessary for left joins
- The lookup table's join columns should be the leading columns in the index

Assumptions:
- Lookup columns at Main table does not contain nulls
- Lookup columns at Lookup table has no duplicate values
- Lookup columns at Main and Lookup tables are of the same data types

### Simple left join to a lookup table containing duplicate values

- Lookup using left join on lookup columns
- Main table has a unique key column with no nulls
- Where return vaue is null, a default value is provided
- The returned column names are renamed with the given names

Expected behaviour:
- Number of rows does not change and remain unique
- Columns returned are added to the main table using the rename column names
- The default values is used where it is specified

Performance considerations:
- Create a composite index on the main table including both the PK and join columns
- The lookup table should have an index on the join columns
- The order of columns in the main table's composite index should match the GROUP BY clause
- The lookup table's join columns should be the leading columns in its index
- GROUP BY columns should not contain nulls for optimal performance

Assumptions:
- Lookup columns at Main table does not contain nulls
- Lookup columns at Main and Lookup tables are of the same data types
- The returning columns cannot be of boolean type

### Simple left join to a lookup table containing duplicate values and nulls in PK and join columns

- Lookup using left join on lookup columns
- Main table has a unique key column with no nulls
- Where return vaue is null, a default value is provided
- The returned column names are renamed with the given names

Expected behaviour:
- Number of rows does not change and remain unique
- Columns returned are added to the main table using the rename column names
- The default values is used where it is specified

Performance considerations:
- Create a composite index on the main table including both the PK and join columns
- The lookup table should have an index on the join columns
- The order of columns in the main table's composite index should match the GROUP BY clause
- The lookup table's join columns should be the leading columns in its index
- GROUP BY columns should not contain nulls for optimal performance

Assumptions:
- Lookup columns at Main and Lookup tables are of the same data types

```sql
with
  main as (
    select
      id, -- PK column
      customer_id,
      col1,
      col2
    from event_table
  ),
  lookup as (
    select
      id, -- PK column
      col1,
      col2
    from customer_table
  )
select
main.*,
lookup.col1 as customer_col1
from main
left join lookup on main.customer_id = lookup.id
```

```sql
with
  main as (
    select
      id, -- PK column
      customer_id,
      category_id,
      col2
    from event_table
  ),
  lookup as (
    select
      id, -- PK column
      category_id, -- non-unique column
      value
    from customer_table
  ),
  lookup_result as (
    select
    main.id,
    lookup.value as category_value
    from main
    left join lookup on main.category_id = lookup.category_id
    group by main.id, main.category_id, lookup.value
  ),
  select
  *
  from main
  join lookup_result using (id)
```
-----------------------

{# ###############################

Wrapper to do nothing on Incremental runs.

1. incremental_drop_all_indexes_on_table() runs drop_all_indexes_on_table()
2. incremental_create_clustered_index(columns) runs create_clustered_index(columns)
3. incremental_create_nonclustered_index(columns) runs create_nonclustered_index(columns)

Example how to use:

    {{
      config({
        "materialized": 'incremental',
        "tags": ["nightly"],
        "as_columnstore": false,
        "unique_key": "QuestionOption_PK",
        "pre-hook": "{{ incremental_drop_all_indexes_on_table() }}",
        "post-hook": [
          "{{ incremental_create_clustered_index(columns = ['Question_Id','QuestionOption_Key']) }}",
          "{{ incremental_create_nonclustered_index(columns = ['Question_TenantId']) }}",
          "{{ incremental_create_nonclustered_index(columns = ['Question_AssessmentDomainId']) }}",
          "{{ incremental_create_nonclustered_index(columns = ['Question_Type','Question_TypeCode']) }}"
        ]
      })
    }}

Please note the quoted curlies!

################################### #}

{% macro incremental_drop_all_indexes_on_table() %}

{# Executed during Parse phase, assemble the SQL String to be passed to Execute phase #}

{{ log("------ is_incremental = " ~ is_incremental()) }}
{% if is_incremental() %}
    {{ log("------ PRE HOOK - Incremental Load -------") }}
    {{ return("------ PRE HOOK - Do nothing") }}
{% else %}
    {{ log("------ PRE HOOK - Initial Load -------")  }}
    {{ log("------ drop_all_indexes_on_table() -------")  }}
    {{ return(drop_all_indexes_on_table()) }}
{% endif %}
{% endmacro %}


{% macro incremental_create_clustered_index(columns) %}

{# Executed during Parse phase, assemble the SQL String to be passed to Execute phase #}

{{ log("------ is_incremental = " ~ is_incremental()) }}
{% if is_incremental() %}
    {{ log("------ POST HOOK - Incremental Load -------")  }}
    {{ return("------ POST HOOK - Do nothing") }}
{% else %}
    {{ log("------ POST HOOK - Initial Load -------")  }}
    {{ log("------ create_clustered_index(columns = " ~ columns ~ ")") }}
    {{ return(create_clustered_index(columns = columns)) }}
{% endif %}

{% endmacro %}


{% macro incremental_create_nonclustered_index(columns, includes=False) %}

{# Executed during Parse phase, assemble the SQL String to be passed to Execute phase #}

{{ log("------ is_incremental = " ~ is_incremental()) }}
{% if is_incremental() %}
    {{ log("------ POST HOOK - Incremental Load -------")  }}
    {{ return("------ POST HOOK - Do nothing") }}
{% else %}
    {{ log("------ POST HOOK - Initial Load -------")  }}
    {{ log("------ create_nonclustered_index(columns = " ~ columns ~ ")") }}
    {{ return(create_nonclustered_index(columns = columns, includes = includes)) }}
{% endif %}

{% endmacro %}
