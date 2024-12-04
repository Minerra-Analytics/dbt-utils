with fact as (
    {{ dbt_utils.stage_sql(ref("fact")) }}
),
lookup_dim_a as (
    {{ dbt_utils.lookup_one(
        main_table='fact', col_to_lookup='dim_a_id',
        lookup_table=ref("dim_a"), lookup_col='dim_a_id',
        return_col='dim_a_sk', rename_col='dim_a_sk',
        lookup_default='0', return_default='0', 
        picker_fn='min'
    )}}
),
lookup_dim_a2 as (
    {{ dbt_utils.lookup_one(
        main_table='lookup_dim_a', col_to_lookup='dim_a_id',
        lookup_table=ref("dim_a"), lookup_col='dim_a_id',
        return_col='dim_a_sk', rename_col='dim_a_sk_first',
        lookup_default='0', return_default='0', 
        picker_fn='first'
    )}}
)
select * from lookup_dim_a2