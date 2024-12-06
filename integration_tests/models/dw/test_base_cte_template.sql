-- test3
{{ dbt_utils.base_cte(ref("dim_a"), clean=false) }}
