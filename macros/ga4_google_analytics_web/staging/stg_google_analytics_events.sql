{% macro create_stg_google_analytics_events() %}

SELECT DISTINCT *
 FROM {{ source('ga4_google_analytics_web', 'events_%')}}

{% endmacro %}