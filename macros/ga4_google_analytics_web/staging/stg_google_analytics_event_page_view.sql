{% macro create_stg_google_analytics_event_page_view(
    reference_name="stg_google_analytics_events"
) %}

    select
        safe_cast(event_date as date format 'YYYYMMDD') as event_date,
        safe_cast(session_value.value.int_value as int) as session_id,
        safe_cast(source_value.value.string_value as string) as session_source,
        safe_cast(medium_value.value.string_value as string) as session_medium,
        safe_cast(device.web_info.hostname as string) as host_name,
        safe_cast(path_value.value.string_value as string) as page_path,
        safe_cast(device.category as string) as device_category,
        safe_cast(null as string) as user_gender,
        safe_cast(null as int) as user_age,
        safe_cast(null as string) as session_default_channel,
        safe_cast(campaign_value.value.string_value as string) as campaign,
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
        safe_cast(null as string) as search_term,
        safe_cast(null as string) as page_referrer,
        safe_cast(coalesce(user_id, user_pseudo_id) as string) as user_id,
        safe_cast(engagement_value.value.int_value as int) as engagement_time_msec

    from {{ ref(reference_name) }}

    cross join unnest(event_params) path_value
    cross join unnest(event_params) source_value
    cross join unnest(event_params) campaign_value
    cross join unnest(event_params) medium_value
    cross join unnest(event_params) engagement_value
    cross join unnest(event_params) session_value
    cross join unnest(event_params) engaged_value

    where
        event_name = 'page_view'
        and path_value.key = 'page_location'
        and source_value.key = 'source'
        and medium_value.key = 'medium'
        and campaign_value.key = 'campaign'
        and engagement_value.key = 'engagement_time_msec'
        and session_value.key = 'ga_session_id'
        and engaged_value.key = 'engaged_session_event'

{% endmacro %}
