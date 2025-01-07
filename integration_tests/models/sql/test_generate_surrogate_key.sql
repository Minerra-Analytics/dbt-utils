
with data as (

    select * from {{ ref('data_generate_surrogate_key') }}

)

select
    {{ dwa.generate_surrogate_key(['column_1']) }} as actual_column_1_only,
    expected_column_1_only,
    {{ dwa.generate_surrogate_key(['column_1', 'column_2', 'column_3']) }} as actual_all_columns_list,
    expected_all_columns

from data
