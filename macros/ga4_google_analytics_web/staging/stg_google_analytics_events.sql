{% macro create_stg_google_analytics_events() %}

SELECT * FROM {{ source('ga4_google_analytics_web', 'events')}}

{% endmacro %}