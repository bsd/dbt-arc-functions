{% macro create_stg_ga4_web_daily_unique_users_rollup(
    reference_name="fct_ga4__sessions_daily"
) %}
    select
        session_partition_date as session_date,
        session_key,
        count(distinct client_key) as unique_users,
        count(client_key) as total_users
    from {{ ref(reference_name) }}
    group by 1, 2
    having unique_users > 0

{% endmacro %}
