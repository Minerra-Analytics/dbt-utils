
with data as (

    select * from {{ ref('data_urls') }}

)

select
    {{ dwarl_parameter('url', 'utm_medium') }} as actual,
    medium as expected

from data

union all

select
    {{ dwarl_parameter('url', 'utm_source') }} as actual,
    source as expected

from data
