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
        person.first_gift_join_source as channel,
        person.join_gift_size_string as join_amount_string,
        case
            when regexp_contains(join_gift_size_string, "0[-]25")
            then 1
            when regexp_contains(join_gift_size_string, "26[-]100")
            then 2
            when regexp_contains(join_gift_size_string, "101[-]250")
            then 3
            when regexp_contains(join_gift_size_string, "251[-]500")
            then 4
            when regexp_contains(join_gift_size_string, "501[-]1000")
            then 5
            when regexp_contains(join_gift_size_string, "1001[-]10000")
            then 6
            when regexp_contains(join_gift_size_string, "10000+")
            then 7
        end join_amount_string_sort,
        sum(transactions.amount) as total_revenue,
        sum(transactions.gift_count) as total_gifts
    from {{ ref(audience_transactions) }} transactions
    left join
        {{ ref(arc_first_gift) }} person on person.person_id = transactions.person_id
    where transactions.coalesced_audience = 'recurring' or recurring = true
    group by 1, 2, 3, 4, 5, 6, 7
    order by 1, 2, 3, 4, 5, 6, 7

{% endmacro %}
