with dim_a as (
    {{ dwa.stage_sql(ref("dim_a")) }}
),
lookup_default as (
    {{ dwa.lookup_one(
        main_table='dim_a', col_to_lookup='id_int',
        lookup_table=ref("tbl_lookup_one_column_id")
    )}}
)
select * from lookup_default
