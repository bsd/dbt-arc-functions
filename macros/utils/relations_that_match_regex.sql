{% macro relations_that_match_regex(
    regex, is_source=False, source_name="", schema_to_search="staging"
) %}
{% set re = modules.re %}
{% set relation_names = [] %}
{% set relations = [] %}
{% set schema_to_search = (
    (target.schema if not is_source else "")
    + ("_" if schema_to_search | length > 0 and not is_source else "")
    + schema_to_search
) %}
{% set table_names = dbt_utils.get_query_results_as_dict(
    "SELECT table_name from "
    + schema_to_search
    + ".INFORMATION_SCHEMA.TABLES"
).table_name %}
{% for table_name in table_names %}
{% if re.match(regex, table_name) %}
{% do relation_names.append(table_name) %}
{% endif %}
{% endfor %}
{{ relation_names }}
{% for relation_name in relation_names %}
{% do relations.append(ref(relation_name)) if not is_source else relations.append(
    source(source_name, relation_name)
) %}
{% endfor %}
{% do return(relations) %}
{% endmacro %}
