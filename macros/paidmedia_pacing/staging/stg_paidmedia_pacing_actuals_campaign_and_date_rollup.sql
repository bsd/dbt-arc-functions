{% macro create_stg_paidmedia_pacing_actuals_campaign_and_date_rollup(
    reference_name="mart_paidmedia_daily_revenue_performance"
) %}

    with
        safe_casting as (
            select
                safe_cast(date_timestamp as date) as date_day,
                lower(safe_cast(campaign_name as string)) as campaign_name,
                safe_cast(spend_amount as int) as spend_amount
            from {{ ref(reference_name) }}

        ),
        daily_spend as (

            select date_day, campaign_name, sum(spend_amount) as daily_spend

            from safe_casting
            group by 1, 2
        ),
        cumulative_spend as (
            select
                date_day,
                campaign_name,
                sum(spend_amount) over (
                    partition by campaign_name order by date_day
                ) as cumulative_spend
            from safe_casting

        )

    select
        daily_spend.date_day,
        daily_spend.campaign_name,
        daily_spend.daily_spend,
        cumulative_spend.cumulative_spend
    from daily_spend
    full outer join
        cumulative_spend
        on daily_spend.date_day = cumulative_spend.date_day
        and daily_spend.campaign_name = cumulative_spend.campaign_name

{% endmacro %}
