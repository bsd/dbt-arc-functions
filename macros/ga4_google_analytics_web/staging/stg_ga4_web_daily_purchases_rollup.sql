{% macro create_stg_ga4_web_daily_purchases_rollup(
    reference_name="fct_ga4__sessions"
) %}
    select
        session_start_date as session_date,
        session_key,
        sum(count_purchases) as purchases
    from {{ ref(reference_name) }}
    group by 1, 2
    having purchases > 0

{% endmacro %}
