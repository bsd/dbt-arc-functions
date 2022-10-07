{% macro create_stg_email_deliverability_person_details_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('^stg_.*_person_details$') %}
{{ dbt_utils.union_relations(relations) }}
{% endmacro %}