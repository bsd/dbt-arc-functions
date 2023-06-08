{% macro create_stg_ga4_session_daily_jobs(
    reference_name='dim_ga4__sessions_daily'
) %}
    with base as (select 
    session_partition_date as session_date,
    session_partition_key as session_key,
    session_default_channel_grouping as default_channel_grouping,
    session_medium,
    session_source,
    session_campaign,
    pages,
    landing_page_hostname as hostname,
    landing_page_path as page_path,
    referrer,
    geo_region,
    device_category,
    device_language
    from
        {{ref(reference_name)}}
        ), 
    deduplicated as (
    select 
    *,
    ROW_NUMBER() OVER (partition by session_date, session_key order by session_date, session_key) as row_num
    from base

)

SELECT
    session_date,
    session_key,
    default_channel_grouping,
    session_medium,
    session_source,
    session_campaign,
    pages,
    hostname,
    page_path,
    referrer,
    geo_region,
    device_category,
    device_language
FROM
    deduplicated
WHERE
    row_num = 1
    
{% endmacro %}
