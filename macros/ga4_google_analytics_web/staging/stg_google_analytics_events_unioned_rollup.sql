{% macro create_stg_google_analytics_events_unioned_rollup(
    reference_name='stg_google_analytics_events_unioned'
) %}

SELECT 
event_date, --1
session_source, --2
session_medium, --3
host_name, --4
REGEXP_EXTRACT(page_path, '/([^/]+)$')  as page_group, --5
session_default_channel, --6
campaign, --7
device_category, --8
device_language, --9
selected_language, --10
user_gender, --11
user_age, --12
accordion_item, --13
accordion_block, --14
file_name, --15
link_domain, --16
top_nav_name, --17
sub_menu_name, --18
search_term, --19
page_referrer, --20
-- block that gets number of events by type
SUM(CASE WHEN event_name = 'accordion_block' 
    THEN 1 else 0 
    END) as accordion_block_clicks,
COUNT(DISTINCT CASE WHEN event_name = 'accordion_block' 
    THEN user_id else NULL 
    END) as unique_accordion_block_clicks,
SUM(CASE WHEN event_name = 'click' 
    THEN 1 else 0 
    END) as clicks,
COUNT(DISTINCT CASE WHEN event_name = 'click' 
    THEN user_id else NULL 
    END) as unique_clicks,
SUM(CASE WHEN event_name = 'file_download' 
    THEN 1 else 0 
    END) as downloads,
COUNT(DISTINCT CASE WHEN event_name = 'file_download' 
    THEN user_id else NULL 
    END) as unique_downloads,
SUM(CASE WHEN event_name = 'first_visit'
    THEN 1 else 0 
    END) as first_visits,
COUNT(DISTINCT CASE WHEN event_name = 'first_visit' 
    THEN user_id else NULL 
    END) as unique_first_visits,
SUM(CASE WHEN event_name = 'language_menu_expand' 
    THEN 1 else 0 
    END) as language_menu_expands,
COUNT(DISTINCT CASE WHEN event_name = 'language_menu_expand' 
    THEN user_id else NULL 
    END) as unique_language_menu_expands,
SUM(CASE WHEN event_name = 'page_views' 
    THEN 1 else 0 
    END) as page_views,
COUNT(DISTINCT CASE WHEN event_name = 'page_views' 
    THEN user_id else NULL 
    END) as unique_page_views,
SUM(CASE WHEN event_name = 'view_search_results' 
    THEN 1 else 0 
    END) as searches,
COUNT(DISTINCT CASE WHEN event_name = 'view_search_results' 
    THEN user_id else NULL 
    END) as unique_searches,
SUM(CASE WHEN event_name = 'scroll' 
    THEN 1 else 0 
    END) as scrolls,
COUNT(DISTINCT CASE WHEN event_name = 'scroll' 
    THEN user_id else NULL 
    END) as unique_scrolls,
SUM(CASE WHEN event_name = 'session_start'
    THEN 1 else 0 
    END) as session_starts, 
COUNT(DISTINCT CASE WHEN event_name = 'session_start' 
    THEN user_id else NULL 
    END) as unique_session_starts,
SUM(CASE WHEN event_name = 'sub_menu_expand' 
    THEN 1 else 0 
    END) as sub_menu_expands,
COUNT(DISTINCT CASE WHEN event_name = 'sub_menu_expand' 
    THEN user_id else NULL 
    END) as unique_sub_menu_expands,
SUM(CASE WHEN event_name = 'user_engagement' 
    THEN 1 else 0 
    END) as user_engagements,
COUNT(DISTINCT CASE WHEN event_name = 'user_engagement' 
    THEN user_id else NULL 
    END) as unique_user_engagements,
SUM(engaged_session) as engaged_sessions,
SUM((CASE WHEN event_name = 'scroll'
 THEN 1 else 0 
 END) * percent_scrolled/(CASE WHEN event_name = 'scroll'
  THEN 1 ELSE 0 END)) as avg_percent_scrolled, 
SUM(engagement_time_msec) as engagement_time_msecs,
COUNT(DISTINCT session_id) as unique_sessions

FROM {{ ref(reference_name) }}
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20


{% endmacro %}




{% endmacro %}
