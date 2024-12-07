  {%- macro debug(msg, info) %}
  {%- set d = var("DEBUG","OFF") %}
  {#- log("DEBUG:'" ~ d ~ "'", info=True) #}
  {#- DEBUG OFF - none, "OFF", false  #}
  {%- if not d %}
  {%- elif d == "OFF" %}
  {%- else %}
  {{-   log(msg, info=True) -}}
  {%- endif %}
  {%- endmacro %}
