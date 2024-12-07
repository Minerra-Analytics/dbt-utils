with
    dim_a as ({{ dbt_utils.stage_sql(ref("dim_a")) }}),
    lookup_max as (
        {{
            dbt_utils.lookup_one(
                main_table="dim_a",
                col_to_lookup="id_int",
                lookup_table=ref("tbl_lookup_one_column_id"),
                lookup_col="id_int",
                return_col="attr_int",
                rename_col="attr_int_max",
                lookup_default="0",
                return_default="0",
                picker_fn="max",
            )
        }}
    )
select *
from lookup_max
