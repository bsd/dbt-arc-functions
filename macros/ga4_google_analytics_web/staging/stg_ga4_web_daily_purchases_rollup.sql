{% macro create_stg_ga4_web_daily_purchases_rollup(
    reference_name="fct_ga4__sessions_daily"
) %}
    select
        session_partition_date as session_date,
        session_key,
        sum(cast(NULL as int64)) as purchases
    from {{ ref(reference_name) }} --value is now missing from ga4 models
    group by 1, 2
    having purchases > 0

{% endmacro %}
