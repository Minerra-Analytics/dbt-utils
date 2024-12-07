with
    {{- dbt_utils.relation_rename_columns(ref('data_people'), cte_name="people") }},
    keyed as (
        select
            people.*,
            {{- dbt_utils.dim_key(["id"], "dim_sk") }}
        from people
    )
select * from keyed
