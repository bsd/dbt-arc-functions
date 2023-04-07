{% macro create_stg_google_analytics_event_accordion_block(
    reference_name='stg_google_analytics_events'
) %}



SELECT 
SAFE_CAST(event_date as DATE FORMAT 'YYYYMMDD') as event_date,
SAFE_CAST(session_value.value.int_value as INT) as session_id,
SAFE_CAST(NULL as STRING) as session_source,
SAFE_CAST(NULL as STRING) as session_medium,
SAFE_CAST(device.web_info.hostname as STRING) as host_name,
SAFE_CAST(path_value.value.string_value as STRING) as page_path, 
SAFE_CAST(device.category as STRING) as device_category,
SAFE_CAST(NULL as STRING) as user_gender,
SAFE_CAST(NULL as INT) as user_age,
SAFE_CAST(NULL as STRING) as session_default_channel,
SAFE_CAST(NULL as STRING) as campaign,
SAFE_CAST(device.language as STRING) as device_language,
SAFE_CAST(NULL as string) as selected_language,
SAFE_CAST(event_name as STRING) as event_name,
SAFE_CAST(accordion_value.value.string_value as STRING) as accordion_item,
SAFE_CAST(accordion_block_value.value.string_value as STRING) as accordion_block,
SAFE_CAST(NULL as STRING) as file_name,
SAFE_CAST(NULL as STRING) as link_domain,
SAFE_CAST(NULL as STRING) as top_nav_name,
SAFE_CAST(NULL as STRING) as sub_menu_name,
SAFE_CAST(engaged_value.value.int_value as INT) as engaged_session,
SAFE_CAST(NULL as INT) as percent_scrolled,
SAFE_CAST(NULL as STRING) as search_term,
SAFE_CAST(refer_value.value.string_value as STRING) as page_referrer, 
SAFE_CAST(COALESCE(user_id, user_pseudo_id) as STRING) as user_id,
SAFE_CAST(engagement_value.value.int_value as INT) as engagement_time_msec

FROM {{ ref(reference_name) }}

 cross join unnest(event_params) path_value
 cross join unnest(event_params) engagement_value
 cross join unnest(event_params) session_value
 cross join unnest(event_params) engaged_value
 cross join unnest(event_params) refer_value
 cross join unnest(event_params) accordion_value
 cross join unnest(event_params) accordion_block_value

 where 
 event_name = 'accordion_block'
 and path_value.key = 'page_location'
 and engagement_value.key = 'engagement_time_msec'
 and session_value.key = 'ga_session_id'
 and engaged_value.key = 'engaged_session_event'
and refer_value.key = 'page_referrer'
and accordion_value.key = 'accordion_item'
and accordion_block_value.key = 'accordion_block'

{% endmacro %}

