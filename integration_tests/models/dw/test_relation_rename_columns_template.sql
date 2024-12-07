-- test3
with
    {{- dbt_utils.relation_rename_columns(ref("dim_a"), clean=false) }}
select *
from base_rename
