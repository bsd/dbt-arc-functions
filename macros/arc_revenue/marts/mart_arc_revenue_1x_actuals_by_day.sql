{% macro create_mart_arc_revenue_1x_actuals_by_day(
    audience_transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched"
) %}


Select
fiscal_year,
{{dbt_arc_functions.get_fiscal_month(
      'transaction_date_day',
      var('fiscal_year_start'))}}
    AS fiscal_month,
extract(month from transaction_date_day) as month,
transaction_date_day,
coalesced_audience as donor_audience,
donor_engagement,
donor_loyalty,
channel_category,
channel,
gift_size_string,
sum(amount) as total_revenue,
sum(gift_count) as total_gifts
from
{{ ref(audience_transactions) }}
where recurring = False or coalesced_audience != 'recurring'
group by 1,2,3,4,5,6,7,8,9,10
order by 1,2,3,4,5,6,7,8,9,10


{% endmacro %}