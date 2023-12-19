-- fmt: off
{% macro util_mart_arc_revenue_actuals_by_day(
    recur_status,
    audience_transactions_table="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    first_gift_table="stg_stitch_sfmc_audience_transaction_first_gift"
) %}

    {% if recur_status not in ["recurring", "onetime"] %}
        {{
            exceptions.raise_compiler_error(
                "'recur_status' argument to util_stg_stitch_sfmc_audience_transaction_rev_by_cohort must be 'recurring' or 'onetime', got "
                ~ recur_status
            )
        }}
    {% endif %}

    {% set recur_suffix = "_recur" if recur_status == "recurring" else "" %}
    {% set recur_boolean = True if recur_status == "recurring" else False %}

    select
        transactions.fiscal_year,
        {{
            dbt_arc_functions.get_fiscal_month(
                "transactions.transaction_date_day", var("fiscal_year_start")
            )
        }} as fiscal_month,
        extract(month from transactions.transaction_date_day) as month,
        transactions.transaction_date_day,
        transactions.coalesced_audience as donor_audience,
        transactions.donor_engagement,
        transactions.donor_loyalty,
        initcap(transactions.channel) as channel,

        {% if recur_status == "onetime" %}
        transactions.gift_size_string,
        case
            when transactions.gift_size_string = "0-25"
            then 1
            when transactions.gift_size_string = "26-100"
            then 2
            when transactions.gift_size_string = "101-250"
            then 3
            when transactions.gift_size_string = "251-500"
            then 4
            when transactions.gift_size_string = "501-1000"
            then 5
            when transactions.gift_size_string = "1001-10000"
            then 6
            when transactions.gift_size_string = "10000+"
            then 7
        end transactions.gift_size_string_sort,

        {% elif recur_status == "recurring"%}

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
        {% endif %}
        sum(transactions.amount) as total_revenue,
        sum(transactions.gift_count) as total_gifts
    from {{ ref(audience_transactions_table) }} transactions
    {% if recur_status == "recurring" %}
    left join
        {{ ref(arc_first_gift) }} person on person.person_id = transactions.person_id
    {% endif %}
    where recurring = {{recur_boolean}}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

{% endmacro %}

