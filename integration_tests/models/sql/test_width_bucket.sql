
with data as (

    select * from {{ ref('data_width_bucket') }}

)

select
    {{ dwa.width_bucket('amount', 'min_value', 'max_value', 'num_buckets') }} as actual,
    bucket as expected

from data
