with
{{- dbt_utils.relation_rename_columns(source("dw_sources","dim_a"), clean=false) }}
