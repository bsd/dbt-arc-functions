{% macro create_mart_arc_revenue_recur_actuals_by_day(
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
CASE
    WHEN REGEXP_CONTAINS(join_gift_size_string,"0[-]25") THEN 1
    WHEN REGEXP_CONTAINS(join_gift_size_string,"26[-]100") THEN 2
    WHEN REGEXP_CONTAINS(join_gift_size_string,"101[-]250") THEN 3
    WHEN REGEXP_CONTAINS(join_gift_size_string,"251[-]500") THEN 4
    WHEN REGEXP_CONTAINS(join_gift_size_string,"501[-]1000") THEN 5
    WHEN REGEXP_CONTAINS(join_gift_size_string,"1001[-]10000") THEN 6
    WHEN REGEXP_CONTAINS(join_gift_size_string,"10000+") THEN 7
END join_amount_string_sort,
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
