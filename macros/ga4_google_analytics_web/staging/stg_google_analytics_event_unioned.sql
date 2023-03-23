{% macro create_stg_google_analytics_event_unioned() %}

{% set relations = dbt_utils.relations_that_match_regex(table_name='^stg_google_analytics_event_%') %}

SELECT DISTINCT * FROM ({{ dbt_utils.union_relations(relations) }})

{% endmacro %}
