{% macro create_mart_cashflow_actuals_and_budget_1x_revenue(
    audience_transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    budget_revenue="stg_audience_budget_by_day_enriched"

) %}

with base as (
    select
        audience_transactions.transaction_date_day as date_day,
        audience_transactions.coalesced_audience as donor_audience,
        lower(audience_transactions.channel) as channel,
        audience_transactions.recurring as recur_flag,
        audience_transactions.fiscal_year,
        sum(audience_transactions.amount) as total_revenue_actuals,
        sum(audience_transactions.gift_count) as total_gifts_actuals
    from
        {{ ref(audience_transactions) }}
            as audience_transactions
    group by 1, 2, 3, 4, 5
    order by 4 desc
),

budget_join as (
    select
        base.recur_flag,
        budget_revenue.total_revenue_budget_by_day
            as total_revenue_budget_by_day,
        coalesce(
            safe_cast(base.fiscal_year as INT64),
            safe_cast(budget_revenue.fiscal_year as INT64)
        ) as fiscal_year,
        coalesce(
            safe_cast(extract(year from base.date_day) as INT64),
            safe_cast(budget_revenue.date_spine_year as INT64)
        ) as year,
        coalesce(
            safe_cast(extract(month from base.date_day) as INT64),
            safe_cast(budget_revenue.date_spine_month as INT64)
        ) as month,
        coalesce(
            safe_cast(extract(day from base.date_day) as INT64),
            safe_cast(budget_revenue.date_spine_day as INT64)
        ) as day,
        coalesce(base.date_day, budget_revenue.date_day) as date_day,
        coalesce(base.donor_audience, budget_revenue.donor_audience)
            as donor_audience,
        coalesce(base.channel, budget_revenue.platform) as channel,
        coalesce(base.total_revenue_actuals, 0) as total_revenue_actuals,
        coalesce(base.total_gifts_actuals, 0) as total_gifts_actuals,
        sum(total_revenue_actuals)
            over (
                partition by
                    coalesce(
                        base.donor_audience, budget_revenue.donor_audience
                    ),
                    coalesce(base.channel, budget_revenue.platform),
                    base.recur_flag,
                    coalesce(base.fiscal_year, budget_revenue.fiscal_year)
                order by
                    coalesce(base.date_day, budget_revenue.date_day)
            )
            as total_revenue_cumulative_fiscal_year
    from base
    full join
        {{ ref(budget_revenue) }} as budget_revenue
        on
            base.date_day = date(budget_revenue.date_day)
            and base.donor_audience = budget_revenue.donor_audience
            and base.channel = budget_revenue.platform
)

select * from budget_join
where recur_flag is null or recur_flag = False
order by 2, 3, 4, 5, 6


{% endmacro %}