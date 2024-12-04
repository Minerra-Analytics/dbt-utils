with dim_a as (
    {{ dbt_utils.stage_sql(ref("dim_a")) }}
),
lookup_b_dupe as (
    {{ dbt_utils.lookup_one(
        main_table='dim_a', col_to_lookup='b_id_int',
        lookup_table=ref("b_dupe"), lookup_col='b_id_int',
        return_col='b_attr_int', rename_col='b_attr_int_min',
        lookup_default='0', return_default='0', 
        picker_fn='min'
    )}}
),
lookup_b_dupe_first as (
    {{ dbt_utils.lookup_one(
        main_table='lookup_b_dupe', col_to_lookup='b_id_int',
        lookup_table=ref("b_dupe"), lookup_col='b_id_int',
        return_col='b_attr_int', rename_col='b_attr_int_first',
        lookup_default='0', return_default='0', 
        picker_fn='first'
    )}}
),
lookup_b_dupe_max as (
    {{ dbt_utils.lookup_one(
        main_table='lookup_b_dupe_first', col_to_lookup='b_id_int',
        lookup_table=ref("b_dupe"), lookup_col='b_id_int',
        return_col='b_attr_int', rename_col='b_attr_int_max',
        lookup_default='0', return_default='0', 
        picker_fn='max'
    )}}
)
select * from lookup_b_dupe_max