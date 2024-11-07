with fact as (
    {{ dbt_utils.stage_sql(ref("fact")) }}
)
select * from fact