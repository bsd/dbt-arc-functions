{% macro create_mart_web_daily_performance(reference_name="mart_web_session_and_daily_performance") %}
select 
session_date,
SUM(page_views) as page_views,
SUM(engagement_time_msec) as engagement_time_msec,
SUM(max_engaged) as max_engaged,
SUM(new_users) as new_users,
SUM(unique_users) as total_users,
sum(page_views) as page_views,
sum(purchases) as purchases,
count(distinct session_key) as total_sessions

from {{ref(reference_name)}}
GROUP BY session_date

{% endmacro %}