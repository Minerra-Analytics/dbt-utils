with dim_a as (
    {{ dbt_utils.stage_sql(ref("dim_a")) }}
),
lookup_first as (
    {{ dbt_utils.lookup_return_many_cols(
        main_table='dim_a', col_to_lookup=['id_int','id_string'],
        lookup_table=ref("tbl_lookup_two_column_id"), lookup_col=['id_int','id_str'],
        return_col=['attr_int', 'attr_string', 'attr_date'], 
        rename_col=['attr_int_min', 'attr_string_min', 'attr_date_min'],
        lookup_default=['0',"''"], 
        return_default=['0', "''", 'NULL'], 
        picker_fn='min'
    )}}
)
select * from lookup_first