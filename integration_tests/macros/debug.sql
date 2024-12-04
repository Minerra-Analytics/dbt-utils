  {%- macro debug(msg, info) %}
  {%- set d = var("DEBUG","OFF") ~ "" %}
  {#- log("DEBUG:'" ~ d ~ "'", info=True) #}
  {%- if d == "OFF" %}
  {%- elif d == "False" %}
  {%- else %}
  {{-   log(msg, info=True) -}}
  {%- endif %}
  {%- endmacro %}


