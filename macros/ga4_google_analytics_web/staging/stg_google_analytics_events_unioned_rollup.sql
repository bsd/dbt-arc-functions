{% macro create_stg_google_analytics_events_unioned_rollup(
    reference_name="stg_google_analytics_events_unioned"
) %}

select
    event_date,  -- 1
    session_source,  -- 2
    session_medium,  -- 3
    host_name,  -- 4
    regexp_extract(page_path, '/([^/]+)$') as page_group,  -- 5
    session_default_channel,  -- 6
    campaign,  -- 7
    device_category,  -- 8
    device_language,  -- 9
    selected_language,  -- 10
    user_gender,  -- 11
    user_age,  -- 12
    accordion_item,  -- 13
    accordion_block,  -- 14
    file_name,  -- 15
    link_domain,  -- 16
    top_nav_name,  -- 17
    sub_menu_name,  -- 18
    search_term,  -- 19
    page_referrer,  -- 20
    -- block that gets number of events by type
    sum(
        case when event_name = 'accordion_block' then 1 else 0 end
    ) as accordion_block_clicks,
    count(
        distinct case when event_name = 'accordion_block' then user_id else null end
    ) as unique_accordion_block_clicks,
    sum(case when event_name = 'click' then 1 else 0 end) as clicks,
    count(
        distinct case when event_name = 'click' then user_id else null end
    ) as unique_clicks,
    sum(case when event_name = 'file_download' then 1 else 0 end) as downloads,
    count(
        distinct case when event_name = 'file_download' then user_id else null end
    ) as unique_downloads,
    sum(case when event_name = 'first_visit' then 1 else 0 end) as first_visits,
    count(
        distinct case when event_name = 'first_visit' then user_id else null end
    ) as unique_first_visits,
    sum(
        case when event_name = 'language_menu_expand' then 1 else 0 end
    ) as language_menu_expands,
    count(
        distinct case
            when event_name = 'language_menu_expand' then user_id else null
        end
    ) as unique_language_menu_expands,
    sum(case when event_name = 'page_views' then 1 else 0 end) as page_views,
    count(
        distinct case when event_name = 'page_views' then user_id else null end
    ) as unique_page_views,
    sum(case when event_name = 'view_search_results' then 1 else 0 end) as searches,
    count(
        distinct case when event_name = 'view_search_results' then user_id else null end
    ) as unique_searches,
    sum(case when event_name = 'scroll' then 1 else 0 end) as scrolls,
    count(
        distinct case when event_name = 'scroll' then user_id else null end
    ) as unique_scrolls,
    sum(case when event_name = 'session_start' then 1 else 0 end) as session_starts,
    count(
        distinct case when event_name = 'session_start' then user_id else null end
    ) as unique_session_starts,
    sum(case when event_name = 'sub_menu_expand' then 1 else 0 end) as sub_menu_expands,
    count(
        distinct case when event_name = 'sub_menu_expand' then user_id else null end
    ) as unique_sub_menu_expands,
    sum(case when event_name = 'user_engagement' then 1 else 0 end) as user_engagements,
    count(
        distinct case when event_name = 'user_engagement' then user_id else null end
    ) as unique_user_engagements,
    sum(engaged_session) as engaged_sessions,
    sum(
        (case when event_name = 'scroll' then 1 else 0 end)
        * percent_scrolled
        / (case when event_name = 'scroll' then 1 else 0 end)
    ) as avg_percent_scrolled,
    sum(engagement_time_msec) as engagement_time_msecs,
    count(distinct session_id) as unique_sessions,
    count(distinct user_id) as unique_users,
    count(user_id) as total_users

from {{ ref(reference_name) }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20

{% endmacro %}
