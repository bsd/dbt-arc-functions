{% macro create_mart_arc_revenue_recur_actuals_by_day.sql(
    audience_transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    arc_first_gift= "stg_stitch_sfmc_audience_transaction_first_gift"
) %}

Select
transactions.fiscal_year,
{{dbt_arc_functions.get_fiscal_month(
      'transactions.transaction_date_day',
      var('fiscal_year_start'))}}
    AS fiscal_month,
extract(month from transactions.transaction_date_day) as month,
transactions.transaction_date_day,
transactions.donor_loyalty,
person.first_gift_join_source as channel,
person.join_gift_size_string as join_amount_string,
sum(transactions.amount) as total_revenue,
sum(transactions.gift_count) as total_gifts
from
{{ ref(audience_transactions) }} transactions
left join
{{ ref(arc_first_gift) }} person
on person.person_id= transactions.person_id
where transactions.coalesced_audience = 'recurring' or recurring = True
group by 1,2,3,4,5,6,7
order by 1,2,3,4,5,6,7

{% endmacro %}