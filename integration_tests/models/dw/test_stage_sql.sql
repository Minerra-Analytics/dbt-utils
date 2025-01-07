with fact as (
    {{ dwa.stage_sql(ref("fact")) }}
)
select * from fact
