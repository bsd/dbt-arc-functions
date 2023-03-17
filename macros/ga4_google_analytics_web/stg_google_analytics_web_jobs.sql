{% macro create_stg_google_analytics_web_jobs(
    reference_name='stg_google_analytics_event_unnest'
) %}

SELECT 
event_date,
session_id,
MIN(session_source) as session_source,
MIN(host_name) as host_name,
MIN(page_path) as page_path,
MIN(device_category) as device_category,
MIN(device_language) as device_language,
MIN(user_gender) as user_gender,
MIN(user_age) as user_age,
MIN(session_default_channel) as session_default_channel,
MIN(campaign) as campaign,
MIN(file_name) as file_name,
MIN(search_term) as search_term


FROM
    {{ ref(reference_name) }}


{% endmacro %}