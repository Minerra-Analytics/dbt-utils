with
    {{- dbt_utils.relation_rename_columns(ref('biz')) }},
    keyed as (
        select
            base.*,
            {{- dbt_utils.fact_key(["id"], "fact_sk") }},
            {{- dbt_utils.dim_date_key("attr_date", "date_sk") }}
        from base
    )
select * from keyed
