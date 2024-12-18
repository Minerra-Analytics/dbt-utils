
{{ dbt_utils.generate_dimension_table_as_sql(
    staging_table=ref("data_people"),
    biz_key='id',
    dw_key='customer_key',
    hash_biz_key=True,
    expose_biz_key=True
) }}
