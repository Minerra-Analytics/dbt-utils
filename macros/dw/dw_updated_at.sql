{% macro dw_updated_at() -%}
{{ debug("dw_updated_at",info=True) }}
to_timestamp(left('{{ run_started_at }}',32),'YYYY-MM-DD HH24:MI:SS.US')::timestamptz as dw_updated_at
{%- endmacro -%}
