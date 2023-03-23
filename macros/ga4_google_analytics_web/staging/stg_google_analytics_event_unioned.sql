{% macro create_stg_google_analytics_event_unioned() %}

{% set relations = dbt_utils.get_relations_by_pattern(target.name, 'stg_google_analytics_event_%') %}

SELECT DISTINCT * FROM ({{ dbt_utils.union_relations(relations) }})

{% endmacro %}


-- Example using the union_relations macro
--{% set event_relations = dbt_utils.get_relations_by_pattern('venue%', 'clicks') %}
--{{ dbt_utils.union_relations(relations = event_relations) }}