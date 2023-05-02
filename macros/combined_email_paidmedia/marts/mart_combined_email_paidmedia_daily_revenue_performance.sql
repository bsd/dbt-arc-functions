{% macro create_mart_combined_email_paidmedia_daily_revenue_performance(
    reference_0_name="mart_email_performance_with_revenue",
    reference_1_name="mart_paidmedia_daily_revenue_performance"
) %}
select
    timestamp_trunc(best_guess_timestamp, day) as date_timestamp,
    'email' as channel,
    best_guess_entity,
    sum(coalesce(total_revenue, 0)) as total_revenue,
    sum(coalesce(total_gifts, 0)) as total_gifts
from {{ ref(reference_0_name) }}
where timestamp_trunc(best_guess_timestamp, day) is not null
group by 1, 2, 3
union all
select
    date_timestamp,
    channel,
    best_guess_entity,
    sum(coalesce(total_revenue, 0)) as total_revenue,
    sum(coalesce(total_gifts, 0)) as total_gifts
from {{ ref(reference_1_name) }}
where date_timestamp is not null
group by 1, 2, 3

{% endmacro %}
