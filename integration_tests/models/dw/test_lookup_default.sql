with dim_a as (
    {{ dbt_utils.stage_sql(ref("dim_a")) }}
),
lookup_default as (
    {{ dbt_utils.lookup_one(
        main_table='dim_a', col_to_lookup='id_int',
        lookup_table=ref("tbl_lookup_one_column_id")
    )}}
)
select * from lookup_default