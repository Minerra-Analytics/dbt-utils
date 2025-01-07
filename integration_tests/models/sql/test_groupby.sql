with test_data as (

    select

        {{ safe_cast("'a'", type_string() )}} as column_1,
        {{ safe_cast("'b'", type_string() )}} as column_2

),

grouped as (

    select
        *,
        count(*) as total

    from test_data
    {{ dwa.group_by(2) }}

)

select * from grouped
