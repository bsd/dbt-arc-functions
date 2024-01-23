{% macro create_mart_cashflow_actuals_and_budget(
    audience_transactions="stg_audience_transactions_and_audience_summary",
    budget_revenue="stg_audience_budget_by_day"
) %}

    with
        base as (
            select
                transaction_date_day as date_day,
                fiscal_year,
                donor_audience,
                channel,
                sum(amount) as total_revenue_actuals,
                sum(gift_count) as total_gifts_actuals
            from {{ ref(audience_transactions) }} as audience_transactions
            group by 1, 2, 3, 4

        ),

        budget_join as (
            select
                coalesce(base.date_day, budget_revenue.date_day) as date_day,
                coalesce(base.fiscal_year, budget_revenue.fiscal_year) as fiscal_year,
                coalesce(
                    base.donor_audience, budget_revenue.donor_audience
                ) as donor_audience,
                coalesce(base.channel, budget_revenue.platform) as channel,
                coalesce(base.total_revenue_actuals, 0) as total_revenue_actuals,
                coalesce(base.total_gifts_actuals, 0) as total_gifts_actuals,
                budget_revenue.total_revenue_budget_by_day,
                sum(total_revenue_actuals) over (
                    partition by
                        coalesce(base.donor_audience, budget_revenue.donor_audience),
                        coalesce(base.channel, budget_revenue.platform),
                        coalesce(base.fiscal_year, budget_revenue.fiscal_year)
                    order by coalesce(base.date_day, budget_revenue.date_day)
                ) as total_revenue_cumulative_fiscal_year
            from base
            full join
                {{ ref(budget_revenue) }} as budget_revenue
                on base.date_day = date(budget_revenue.date_day)
                and lower(base.donor_audience) = lower(budget_revenue.donor_audience)
                and lower(base.channel) = lower(budget_revenue.platform)
        ),

        dateoffset as (
            select
                *,
                date_sub(date_day, interval 1 year) as prev_year_date_day,
                date_sub(date_day, interval 2 year) as prev_two_year_date_day
            from budget_join
        ),

        prevyear as (
            select
                donor_audience,
                channel,
                date_day,
                total_revenue_actuals as prev_year_total_revenue_actuals,
                total_revenue_budget_by_day as prev_year_total_revenue_budget
            from dateoffset
        ),

        prevtwoyears as (
            select
                donor_audience,
                channel,
                date_day,
                total_revenue_actuals as prev_two_year_total_revenue_actuals,
                total_revenue_budget_by_day as prev_two_year_total_revenue_budget
            from dateoffset
        ),
        enriched as (
            select
                dateoffset.date_day as date_day,
                date_add(prevyear.date_day, interval 1 year) as prevyear_date_day,
                date_add(
                    prevtwoyears.date_day, interval 2 year
                ) as prevtwoyears_date_day,
                coalesce(
                    dateoffset.donor_audience,
                    prevyear.donor_audience,
                    prevtwoyears.donor_audience
                ) as donor_audience,
                coalesce(
                    dateoffset.channel, prevyear.channel, prevtwoyears.channel
                ) as channel,
                dateoffset.total_revenue_actuals,
                dateoffset.total_gifts_actuals,
                dateoffset.total_revenue_budget_by_day,
                dateoffset.total_revenue_cumulative_fiscal_year,
                prevyear.prev_year_total_revenue_actuals,
                prevyear.prev_year_total_revenue_budget,
                prevtwoyears.prev_two_year_total_revenue_actuals,
                prevtwoyears.prev_two_year_total_revenue_budget
            from dateoffset
            full outer join
                prevyear
                on dateoffset.donor_audience = prevyear.donor_audience
                and dateoffset.prev_year_date_day = prevyear.date_day
                and dateoffset.channel = prevyear.channel
            full outer join
                prevtwoyears
                on dateoffset.donor_audience = prevtwoyears.donor_audience
                and dateoffset.prev_two_year_date_day = prevtwoyears.date_day
                and dateoffset.channel = prevtwoyears.channel
        )
    select
        coalesce(date_day, prevyear_date_day, prevtwoyears_date_day) as date_day,
        donor_audience,
        channel,
        sum(total_revenue_actuals) as total_revenue_actuals,
        sum(total_revenue_budget_by_day) as total_revenue_budget_by_day,
        max(
            total_revenue_cumulative_fiscal_year
        ) as total_revenue_cumulative_fiscal_year,
        sum(prev_year_total_revenue_actuals) as prev_year_total_revenue_actuals,
        sum(prev_year_total_revenue_budget) as prev_year_total_revenue_budget,
        sum(prev_two_year_total_revenue_actuals) as prev_two_year_total_revenue_actuals,
        sum(prev_two_year_total_revenue_budget) as prev_two_year_total_revenue_budget
    from enriched
    group by 1, 2, 3

{% endmacro %}
