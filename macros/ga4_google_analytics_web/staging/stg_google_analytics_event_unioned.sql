{% macro create_stg_google_analytics_event_unioned() %}

{% set relations = dbt_utils.get_relations_by_pattern ( 
    schema_pattern = target.name,
    table_pattern = 'stg_google_analytics_event_*',
    database = target.database)
%}

SELECT DISTINCT * FROM ({{ dbt_utils.union_relations(relations) }})

{% endmacro %}