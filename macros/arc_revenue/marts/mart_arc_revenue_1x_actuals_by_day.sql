{% macro create_mart_arc_revenue_1x_actuals_by_day(
    audience_transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched"
) %}

    select
        fiscal_year,
        {{
            dbt_arc_functions.get_fiscal_month(
                "transaction_date_day", var("fiscal_year_start")
            )
        }} as fiscal_month,
        extract(month from transaction_date_day) as month,
        transaction_date_day,
        coalesced_audience as donor_audience,
        donor_engagement,
        donor_loyalty,
        channel,
        gift_size_string,
        case
            when gift_size_string = "0-25"
            then 1
            when gift_size_string = "26-100"
            then 2
            when gift_size_string = "101-250"
            then 3
            when gift_size_string = "251-500"
            then 4
            when gift_size_string = "501-1000"
            then 5
            when gift_size_string = "1001-10000"
            then 6
            when gift_size_string = "10000+"
            then 7
        end gift_size_string_sort,
        sum(amount) as total_revenue,
        sum(gift_count) as total_gifts
    from {{ ref(audience_transactions) }}
    where recurring = false and coalesced_audience != 'Monthly'
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

{% endmacro %}
