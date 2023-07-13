{% macro create_stg_ga4_web_daily_engagement_time_rollup(
    reference_name="fct_ga4__sessions_daily"
) %}
    select
        session_partition_date as session_date,
        session_key,
        sum(session_partition_sum_engagement_time_msec) as engagement_time_msec
    from {{ ref(reference_name) }}
    group by 1, 2
    having engagement_time_msec > 0

{% endmacro %}
