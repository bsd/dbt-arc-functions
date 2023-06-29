{% macro create_mart_web_session_and_daily_performance(
    jobs="stg_web_daily_jobs_unioned",
    engagement_time="stg_web_daily_engagement_time_rollup_unioned",
    max_engaged="stg_web_daily_max_engaged_rollup_unioned",
    new_users="stg_web_daily_new_users_rollup_unioned",
    unique_users="stg_web_daily_unique_users_rollup_unioned",
    page_views="stg_web_daily_page_views_rollup_unioned",
    purchases="stg_web_daily_purchases_rollup_unioned"
) %}
    select
        jobs.session_key,
        jobs.session_date,
        jobs.default_channel_grouping,
        jobs.session_medium,
        jobs.session_source,
        jobs.session_campaign,
        jobs.hostname,
        jobs.page_path,
        case
            when page_path = "/" then "/" else regexp_extract(page_path, "^(/[^/]+)")
        end as first_directory,
        jobs.referrer,
        jobs.geo_region,
        jobs.device_category,
        jobs.device_language,
        engagement_time.engagement_time_msec,
        max_engaged.max_engaged,
        new_users.new_users,
        unique_users.unique_users,
        page_views.page_views,
        purchases.purchases
    from {{ ref(jobs) }} jobs
    full join
        {{ ref(engagement_time) }} engagement_time
        on jobs.session_key = engagement_time.session_key
        and jobs.session_date = engagement_time.session_date
    full join
        {{ ref(max_engaged) }} max_engaged
        on jobs.session_key = max_engaged.session_key
        and jobs.session_date = max_engaged.session_date
    full join
        {{ ref(new_users) }} new_users
        on jobs.session_key = new_users.session_key
        and jobs.session_date = new_users.session_date
    full join
        {{ ref(unique_users) }} unique_users
        on jobs.session_key = unique_users.session_key
        and jobs.session_date = unique_users.session_date
    full join
        {{ ref(page_views) }} page_views
        on jobs.session_key = page_views.session_key
        and jobs.session_date = page_views.session_date
    full join
        {{ ref(purchases) }} purchases
        on jobs.session_key = purchases.session_key
        and jobs.session_date = purchases.session_date
    order by 1 desc, 2
{% endmacro %}
