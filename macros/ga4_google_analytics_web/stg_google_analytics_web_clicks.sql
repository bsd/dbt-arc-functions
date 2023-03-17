{% macro create_stg_google_analytics_web_clicks(
    reference_name='stg_google_analytics_event_unnest'
) %}

SELECT 
event_date,
session_id,
SUM(CASE WHEN event_name = 'click' THEM 1 ELSE 0 END) AS clicks
FROM {{ ref(reference_name) }}
GROUP BY 1, 2