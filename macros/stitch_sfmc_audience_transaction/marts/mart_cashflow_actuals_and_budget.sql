{% macro create_mart_cashflow_actuals_and_budget(
    audience_transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    budget_revenue="stg_audience_budget_by_day_enriched"
) %}

    with
        base as (
            select
                audience_transactions.transaction_date_day as date_day,
                audience_transactions.coalesced_audience as donor_audience,
                lower(audience_transactions.channel) as channel,
                audience_transactions.fiscal_year,
                sum(audience_transactions.amount) as total_revenue_actuals,
                sum(audience_transactions.gift_count) as total_gifts_actuals
            from {{ ref(audience_transactions) }} as audience_transactions
            group by 1, 2, 3, 4
            order by 4 desc
        )

    select
        coalesce(
            safe_cast(base.fiscal_year as int64),
            safe_cast(budget_revenue.fiscal_year as int64)
        ) as fiscal_year,
        coalesce(
            safe_cast(extract(year from base.date_day) as int64),
            safe_cast(budget_revenue.date_spine_year as int64)
        ) as year,
        coalesce(
            safe_cast(extract(month from base.date_day) as int64),
            safe_cast(budget_revenue.date_spine_month as int64)
        ) as month,
        coalesce(
            safe_cast(extract(day from base.date_day) as int64),
            safe_cast(budget_revenue.date_spine_day as int64)
        ) as day,
        coalesce(base.date_day, budget_revenue.date_day) as date_day,
        coalesce(base.donor_audience, budget_revenue.donor_audience) as donor_audience,
        coalesce(base.channel, budget_revenue.platform) as channel,
        coalesce(base.total_revenue_actuals, 0) as total_revenue_actuals,
        coalesce(base.total_gifts_actuals, 0) as total_gifts_actuals,
        budget_revenue.total_revenue_budget_by_day as total_revenue_budget_by_day,
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
        and base.donor_audience = budget_revenue.donor_audience
        and base.channel = budget_revenue.platform
    order by 2, 3, 4, 5, 6

{% endmacro %}
