{% macro create_stg_google_analytics_event_unnest(
    reference_name='stg_google_analytics_event'
) %}

SELECT 
SAFE_CAST(event_date as DATE FORMAT 'YYYYMMDD') as event_date,
SAFE_CAST(session_value.value.int_value as INT) as session_id,
SAFE_CAST(source_value.value.string_value as STRING) as session_source,
SAFE_CAST(device.web_info.hostname as STRING) as host_name,
SAFE_CAST(path_value.value.string_value as STRING) as page_path,
SAFE_CAST(device.category as STRING) as device_category,
SAFE_CAST(NULL as STRING) as user_gender,
SAFE_CAST(NULL as INT) as user_age,
SAFE_CAST(NULL as STRING) as session_default_channel,
SAFE_CAST(campaign_value.value.string_value as STRING) as campaign,
SAFE_CAST(device.language as STRING) as device_language,
SAFE_CAST(event_name as STRING) as event_name,
SAFE_CAST(NULL as STRING) as accordion_item,
SAFE_CAST(NULL as STRING) as accordion_block,
SAFE_CAST(NULL as STRING) as file_name,
SAFE_CAST(NULL as STRING) as search_term,
SAFE_CAST(COALESCE(user_id, user_pseudo_id) as STRING) as user_id,
SAFE_CAST(engagement_value.value.int_value as INT) as engagement_time_msec

FROM {{ ref(reference_name) }}

 cross join unnest(event_params) path_value
 cross join unnest(event_params) source_value
 cross join unnest(event_params) campaign_value
 cross join unnest(event_params) engagement_value
 cross join unnest(event_params) session_value

 where path_value.key = 'page_location'
 and source_value.key = 'source'
 and campaign_value.key = 'campaign'
 and engagement_value.key = 'engagement_time_msec'
 and session_value.key = 'ga_session_id'

{% endmacro %}

