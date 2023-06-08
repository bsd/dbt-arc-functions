{% macro create_stg_ga4_web_daily_page_views_rollup(
    reference_name="fct_ga4__sessions_daily)"
) %}
    select
        session_partition_date as session_date,
        session_partition_key as session_key,
        sum(session_partition_count_page_views) as page_views
    from {{ ref(reference_name) }}
    group by 1, 2

{% endmacro %}
