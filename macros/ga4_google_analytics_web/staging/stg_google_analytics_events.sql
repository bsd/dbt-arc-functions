{% macro create_stg_google_analytics_events() %}

    select * from {{ source("ga4_google_analytics_web", "events") }}

{% endmacro %}
