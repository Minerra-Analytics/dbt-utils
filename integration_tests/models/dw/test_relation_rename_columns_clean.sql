-- test8
with
{{- dbt_utils.relation_rename_columns(ref("dim_a"), clean=true) }},
    rename as (
        select
            "dim_a_sk",
            "dim_a_id",
            "a_id",
            "dim_attr_int",
            "dim_attr_string",
            "dim_str_date",
            "id_int",
            "id_string",
            "id_date"
        from base
    )
select *
from rename
