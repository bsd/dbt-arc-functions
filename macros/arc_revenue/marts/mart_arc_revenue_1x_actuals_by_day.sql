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
CASE
    WHEN gift_size_string = "0-25" THEN 1
    WHEN gift_size_string = "26-100" THEN 2
    WHEN gift_size_string = "101-250" THEN 3
    WHEN gift_size_string = "251-500" THEN 4
    WHEN gift_size_string = "501-1000" THEN 5
    WHEN gift_size_string = "1001-10000" THEN 6
    WHEN gift_size_string = "10000+" THEN 7
END gift_size_string_sort,
sum(amount) as total_revenue,
sum(gift_count) as total_gifts
from
{{ ref(audience_transactions) }}
where recurring = False or coalesced_audience != 'recurring'
group by 1,2,3,4,5,6,7,8,9,10
order by 1,2,3,4,5,6,7,8,9,10


{% endmacro %}
