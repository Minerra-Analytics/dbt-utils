{#
    Macro: relation_exist

    Description:
    This macro checks if a given relation (table) exists in the database schema.
    It utilizes the `postgres__list_relations_without_caching` function to retrieve
    a list of relations and iterates through them to find a match based on the
    relation's identifier and schema.

    Parameters:
    - relation: The relation object containing the identifier and schema to be checked.

    Returns:
    - Boolean: True if the relation is found, False otherwise.

    Notes:
    In Jinja2, variables are immutable (like CONST).
    This means that once a variable is set, its value cannot be changed directly.
    However, dictionaries in Python (and by extension in Jinja2) are mutable, meaning their contents can be changed without reassigning the variable itself.
#}
{%- macro relation_exist(relation) %}
{%- set table_list = postgres__list_relations_without_caching(relation) %}
{%- set flag = { "is_found": False } %}
{%- for row in table_list if row[1] == relation.identifier and row[2] == relation.schema %}
{%-   if flag.update({ "is_found": True }) %}{%- endif %}
{%- endfor %}
{{- return(flag.is_found) }}
{%- endmacro %}
