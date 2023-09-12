{% macro create_mart_cashflow_actuals_and_budget_1x_revenue(
    audience_transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    budget_revenue="stg_audience_budget_by_day_enriched"
) %}

    with
        base as (
            select
                audience_transactions.transaction_date_day as date_day,
                audience_transactions.coalesced_audience as donor_audience,
                lower(audience_transactions.channel) as channel,
                audience_transactions.recurring as recur_flag,
                audience_transactions.fiscal_year,
                sum(audience_transactions.amount) as total_revenue_actuals,
                sum(audience_transactions.gift_count) as total_gifts_actuals
            from {{ ref(audience_transactions) }} as audience_transactions
            group by 1, 2, 3, 4, 5
            order by 4 desc
        ),

        budget_join as (
            select
                base.recur_flag,
                budget_revenue.total_revenue_budget_by_day
                as total_revenue_budget_by_day,
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
                coalesce(
                    base.donor_audience, budget_revenue.donor_audience
                ) as donor_audience,
                coalesce(base.channel, budget_revenue.platform) as channel,
                coalesce(base.total_revenue_actuals, 0) as total_revenue_actuals,
                coalesce(base.total_gifts_actuals, 0) as total_gifts_actuals,
                sum(total_revenue_actuals) over (
                    partition by
                        coalesce(base.donor_audience, budget_revenue.donor_audience),
                        coalesce(base.channel, budget_revenue.platform),
                        base.recur_flag,
                        coalesce(base.fiscal_year, budget_revenue.fiscal_year)
                    order by coalesce(base.date_day, budget_revenue.date_day)
                ) as total_revenue_cumulative_fiscal_year
            from base
            full join
                {{ ref(budget_revenue) }} as budget_revenue
                on base.date_day = date(budget_revenue.date_day)
                and base.donor_audience = budget_revenue.donor_audience
                and base.channel = budget_revenue.platform
        ),
        original_mart as (
            select *
            from budget_join
            where recur_flag is null or recur_flag = false
            order by 2, 3, 4, 5, 6
        ),
        dateoffset as (
            select
                *,
                date_sub(date_day, interval 1 year) as prev_year_date_day,
                date_sub(date_day, interval 2 year) as prev_two_year_date_day
            from original_mart
        ),

        prevyear as (
            select
                donor_audience,
                date_day,
                total_revenue_actuals as prev_year_total_revenue_actuals,
                total_revenue_budget_by_day as prev_year_total_revenue_budget
            from dateoffset
        ),

        prevtwoyears as (
            select
                donor_audience,
                date_day,
                total_revenue_actuals as prev_two_year_total_revenue_actuals,
                total_revenue_budget_by_day as prev_two_year_total_revenue_budget
            from dateoffset
        )
, enriched as (
    select
        dateoffset.*,
        prevyear.prev_year_total_revenue_actuals,
        prevyear.prev_year_total_revenue_budget,
        prevtwoyears.prev_two_year_total_revenue_actuals,
        prevtwoyears.prev_two_year_total_revenue_budget
    from dateoffset 
    left join
        prevyear
        on dateoffset.donor_audience = prevyear.donor_audience
        and dateoffset.prev_year_date_day = prevyear.date_day  -- Same day last year
    left join
        prevtwoyears 
        on dateoffset.donor_audience = prevtwoyears.donor_audience
        and dateoffset.prev_two_year_date_day = prevtwoyears.date_day  -- Same day two years back
        )

select 
year,
month,
day,
date_day, 
fiscal_year,
donor_audience, 
channel,
total_revenue_actuals,
total_gifts_actuals,
total_revenue_budget_by_day,
total_revenue_cumulative_fiscal_year,
prev_year_total_revenue_actuals,
prev_year_total_revenue_budget,
prev_two_year_total_revenue_actuals,
prev_two_year_total_revenue_budget
from enriched



{% endmacro %}
