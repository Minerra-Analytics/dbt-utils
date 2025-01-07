{{ config(materialized = 'table') }}

-- depends_on: {{ ref('data_events_20180101') }}, {{ ref('data_events_20180102') }}, {{ ref('data_events_20180103') }}

{% set relations = dwa.get_relations_by_prefix(target.schema, 'data_events_') %}
{{ dwa.union_relations(relations) }}
