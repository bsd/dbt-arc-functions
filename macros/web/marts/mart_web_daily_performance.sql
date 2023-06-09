{% macro create_mart_web_daily_performance(
    reference_name="mart_web_session_and_daily_performance"
) %}
    select
        session_date,
        sum(page_views) as page_views,
        sum(engagement_time_msec) as engagement_time_msec,
        sum(max_engaged) as max_engaged,
        sum(new_users) as new_users,
        sum(unique_users) as total_users,
        sum(purchases) as purchases,
        count(distinct session_key) as total_sessions

    from {{ ref(reference_name) }}
    group by session_date

{% endmacro %}
