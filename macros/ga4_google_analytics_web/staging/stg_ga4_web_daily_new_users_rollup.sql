{% macro create_stg_ga4_web_daily_new_users_rollup(
    reference_name="dim_ga4__sessions"
) %}
    select session_start_date as session_date, session_key, count(*) as new_users
    from {{ ref(reference_name) }}
    where is_first_session = true
    group by 1, 2
    having new_users > 0

{% endmacro %}
