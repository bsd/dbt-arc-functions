{% macro create_stg_stitch_sfmc_arc_revenue_and_donor_count_by_lifetime_gifts(
    reference_name="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched"
) %}

with base as (
    select
        transaction_date_day as transaction_date_day,
        coalesced_audience as donor_audience,
        channel,
        SUM(gift_count) as gift_count,
        count(distinct person_id) as donors,
        sum(amount) as amount
    from {{ ref(reference_name) }}
    where recurring = true
    group by 1, 2, 3
    order by 1, 2, 3

)

, cumulative_base as (
    select 
    transaction_date_day,
    donor_audience,
    channel,
    donors
    amount,
    sum(gift_count) OVER (ORDER BY transaction_date_day) AS cumulative_gift_count
    from base
    group by 1, 2, 3, 4, 5
)

select 
transaction_date_day,
donor_audience,
channel,
donors,
summed_amount,
cumulative_gift_count,
case
    when cumulative_gift_count < 6
    then "less than 6"
    when cumulative_gift_count between 6 and 12
    then "6-12"
    when cumulative_gift_count between 13 and 24
    then "13-24"
    when cumulative_gift_count between 25 and 36
    then "25-36"
    else "37+"
end as recurring_gift_cumulative_str
from base

{% endmacro %}
