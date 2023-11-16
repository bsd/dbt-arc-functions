{% macro create_stg_stitch_sfmc_arc_revenue_and_donor_count_by_lifetime_gifts(
    reference_name="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched"
) %}
    select
        extract(year from transaction_date_day) as transaction_date_year,
        extract(month from transaction_date_day) as transaction_date_month,
        transaction_date_day as transaction_date_day,
        case
            when gift_count < 6
            then "less than 6"
            when gift_count between 6 and 12
            then "6-12"
            when gift_count between 13 and 24
            then "13-24"
            when gift_count between 25 and 36
            then "25-36"
            else "37+"
        end as recurring_gift_cumulative_str,
        count(distinct person_id) as donors,
        sum(amount) as summed_amount,
    from {{ ref(reference_name) }}
    where recurring = true
    group by 1, 2, 3, 4
    order by 1, 2, 3, 4

{% endmacro %}
