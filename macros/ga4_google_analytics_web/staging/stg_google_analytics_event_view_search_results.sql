{% macro create_stg_google_analytics_event_view_search_results(
    reference_name="stg_google_analytics_events"
) %}

select
    safe_cast(event_date as date format 'YYYYMMDD') as event_date,
    safe_cast(session_value.value.int_value as int) as session_id,
    safe_cast(null as string) as session_source,
    safe_cast(null as string) as session_medium,
    safe_cast(device.web_info.hostname as string) as host_name,
    safe_cast(path_value.value.string_value as string) as page_path,
    safe_cast(device.category as string) as device_category,
    safe_cast(null as string) as user_gender,
    safe_cast(null as int) as user_age,
    safe_cast(null as string) as session_default_channel,
    safe_cast(null as string) as campaign,
    safe_cast(device.language as string) as device_language,
    safe_cast(null as string) as selected_language,
    safe_cast(event_name as string) as event_name,
    safe_cast(null as string) as accordion_item,
    safe_cast(null as string) as accordion_block,
    safe_cast(null as string) as file_name,
    safe_cast(null as string) as link_domain,
    safe_cast(null as string) as top_nav_name,
    safe_cast(null as string) as sub_menu_name,
    safe_cast(engaged_value.value.int_value as int) as engaged_session,
    safe_cast(null as int) as percent_scrolled,
    safe_cast(search_value.value.string_value as string) as search_term,
    safe_cast(refer_value.value.string_value as string) as page_referrer,
    safe_cast(coalesce(user_id, user_pseudo_id) as string) as user_id,
    safe_cast(null as int) as engagement_time_msec

from {{ ref(reference_name) }}

cross join unnest(event_params) path_value
cross join unnest(event_params) session_value
cross join unnest(event_params) engaged_value
cross join unnest(event_params) search_value
cross join unnest(event_params) refer_value

where
    event_name = 'view_search_results'
    and path_value.key = 'page_location'
    and session_value.key = 'ga_session_id'
    and engaged_value.key = 'engaged_session_event'
    and search_value.key = 'search_term'
    and refer_value.key = 'page_referrer'

{% endmacro %}
