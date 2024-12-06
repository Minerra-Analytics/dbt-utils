-- test3
{{ dbt_utils.base_cte(ref("dim_a"), except=['id_int', 'id_string'], clean=false) }}
