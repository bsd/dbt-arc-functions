{% macro create_mart_google_analytics_web_performance_by_date(
    reference_name='stg_google_analytics_events_unioned_rollup'
) %}

SELECT DISTINCT * FROM {{ ref(reference_name)}}
ORDER BY event_date desc


{% endmacro %}
