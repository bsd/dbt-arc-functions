{% macro create_mart_cashflow_actuals_and_budget(
    audience_transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    budget_revenue="stg_audience_budget_by_day_enriched"

) %}




with base as (
    select
        audience_transactions.transaction_date_day as date_day,
        audience_transactions.coalesced_audience as donor_audience,
        audience_transactions.inbound_channel as channel,
        audience_transactions.recurring as recur_flag,
        sum(audience_transactions.amount) as total_revenue_actuals,
        sum(audience_transactions.gift_count) as total_gifts_actuals,
        sum(budget_revenue.total_revenue_budget_by_day) as total_revenue_budget

    from
        {{ ref(audience_transactions) }}
            as audience_transactions
    left join
        {{ ref(budget_revenue) }} as budget_revenue
        on
            audience_transactions.transaction_date_day
            = date(budget_revenue.date_day)
            and audience_transactions.coalesced_audience
            = budget_revenue.donor_audience
    group by 1, 2, 3, 4
    order by 4 desc
)

select
    extract(month from date_day) as month,
    extract(year from date_day) as year,
  {{ dbt_arc_functions.get_fiscal_year(
      'date_day',
      var('fiscal_year_start')) }}
    AS fiscal_year,
    *
from base

{% endmacro %}