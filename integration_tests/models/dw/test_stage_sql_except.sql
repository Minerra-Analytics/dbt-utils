with fact as (
    {{ dbt_utils.stage_sql(ref("fact"), except=["attr_date"]) }}
)
select * from fact