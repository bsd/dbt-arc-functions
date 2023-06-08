{% macro create_stg_ga4_web_daily_jobs(reference_name="dim_ga4__sessions_daily") %}
    with
        base as (
            select
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
            from {{ ref(reference_name) }}
        ),
        deduplicated as (
            select
                *,
                row_number() over (
                    partition by session_date, session_key
                    order by session_date, session_key
                ) as row_num
            from base

        )

    select
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
    from deduplicated
    where row_num = 1

{% endmacro %}
