-- test4
with
{{- dbt_utils.relation_rename_columns(ref("dim_a"), except=['id_int', 'id_string'], clean=False) }}
