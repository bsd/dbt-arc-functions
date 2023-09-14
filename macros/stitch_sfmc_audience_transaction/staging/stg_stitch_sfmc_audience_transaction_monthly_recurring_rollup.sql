{% macro create_stg_stitch_sfmc_audience_transaction_monthly_recurring_rollup(
    reference_name="stg_stitch_sfmc_audience_transaction_with_join_date"
) %}
    select
        date_trunc(transaction_date_day, month) as transaction_month_year_date,
        date_trunc(join_month_year_date, month) as join_month_year,
        donor_audience,
        channel,
        join_gift_size_string,
        sum(amount) as total_revenue,
        safe_cast(count(distinct person_id) as integer) as total_donors
    from {{ ref(reference_name) }}
    where recurring = true  -- boolean 
    group by 1, 2, 3, 4, 5
    order by 1, 2, 3, 4, 5

{% endmacro %}
