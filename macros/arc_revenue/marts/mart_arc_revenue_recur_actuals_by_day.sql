{% macro create_mart_arc_revenue_recur_actuals_by_day(
    audience_transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    arc_first_gift="stg_stitch_sfmc_audience_transaction_first_gift"
) %}

    select
        transactions.fiscal_year,
        {{
            dbt_arc_functions.get_fiscal_month(
                "transactions.transaction_date_day", var("fiscal_year_start")
            )
        }} as fiscal_month,
        extract(month from transactions.transaction_date_day) as month,
        transactions.transaction_date_day,
        transactions.donor_loyalty,
        transactions.coalesced_audience as donor_audience,
        person.first_gift_join_source as channel,
        person.join_gift_size_string_recur as join_amount_string_recur,
        case
            person.join_gift_size_string_recur
            when '0-10'
            then 1
            when '11-20'
            then 2
            when '21-30'
            then 3
            when '31-40'
            then 4
            when '41-50'
            then 5
            when '51-60'
            then 6
            when '61-70'
            then 7
            when '71-80'
            then 8
            when '81-90'
            then 9
            when '91-100'
            then 10
            when '100+'
            then 11
        end join_amount_string_recur_sort,
        sum(transactions.amount) as total_revenue,
        sum(transactions.gift_count) as total_gifts
    from {{ ref(audience_transactions) }} transactions
    left join
        {{ ref(arc_first_gift) }} person on person.person_id = transactions.person_id
    where recurring = true
    group by 1, 2, 3, 4, 5, 6, 7, 8
    order by 1, 2, 3, 4, 5, 6, 7, 8

{% endmacro %}
