{% macro create_stg_google_analytics_event_unioned() %}

{{ dbt_utils.union_relations(

{% set relations = dbt_utils.get_relations_by_pattern ( 
    schema_to_search = target.name,
    table_pattern = 'stg_google_analytics_event_%',
    database = target.database
)%}

) }}

{% endmacro %}