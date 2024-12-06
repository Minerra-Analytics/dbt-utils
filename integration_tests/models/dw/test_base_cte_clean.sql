-- test3
{{ dbt_utils.base_cte(ref("dim_a"), clean=true) }},
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
