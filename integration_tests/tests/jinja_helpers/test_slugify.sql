with comparisons as (
  select '{{ dwafy("") }}' as output, '' as expected
  union all
  select '{{ dwafy(None) }}' as output, '' as expected
  union all
  select '{{ dwafy("!Hell0 world-hi") }}' as output, 'hell0_world_hi' as expected
  union all
  select '{{ dwafy("0Hell0 world-hi") }}' as output, '_0hell0_world_hi' as expected
)

select *
from comparisons
where output != expected
