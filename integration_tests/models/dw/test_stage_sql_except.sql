with fact as (
    {{ dwa.stage_sql(ref("fact"), except=["attr_date"]) }}
)
select * from fact
