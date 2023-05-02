{% macro create_stg_google_analytics_events_unioned() %}

{% set relations = dbt_arc_functions.relations_that_match_regex(
    "^stg_google_analytics_event_[a-zA-Z_]+$"
) %}
{{ dbt_utils.union_relations(relations) }}

{% endmacro %}
