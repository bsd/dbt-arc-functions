{% macro create_mart_google_analytics_web_performance_by_date(
    reference_name="stg_google_analytics_events_unioned_rollup"
) %}

    select distinct * from {{ ref(reference_name) }} order by event_date desc

{% endmacro %}
