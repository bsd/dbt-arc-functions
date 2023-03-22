{% macro create_stg_google_analytics_event_unioned() %}

{{ dbt_utils.union_relations(

{% set relations = dbt_utils.get_relations_by_pattern (

    
)%}

) }}

{% endmacro %}