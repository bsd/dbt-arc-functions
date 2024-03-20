{% macro create_stg_paidmedia_goals_monthly_actuals_rollup(
    reference_name="mart_paidmedia_daily_revenue_performance"
) %}
    select
        format_date('%B %Y', cast(date_timestamp as date)) as month_year,
        case
            when channel_type is not null
            then
                safe_cast(
                    lower(channel_category) || ' ' || lower(channel_type) as string
                )
            else safe_cast(lower(channel_category) as string)
        end as channel,
        safe_cast(lower(objective) as string) as objective,
        safe_cast(lower(channel) as string) as platform,
        safe_cast(sum(spend_amount) as float64) as actual_spend,
        safe_cast(sum(total_revenue) as float64) as actual_revenue,
        safe_cast(sum(total_gifts) as float64) as actual_donations,
        safe_cast(sum(one_time_revenue) as float64) as actual_1x_revenue,
        safe_cast(sum(one_time_gifts) as float64) as actual_1x_gifts,
        safe_cast(sum(total_monthly_revenue) as float64) as actual_monthly_revenue,
        safe_cast(sum(total_monthly_gifts) as float64) as actual_monthly_gifts,
        
    from {{ ref(reference_name) }}
    group by 1, 2, 3, 4
{% endmacro %}
