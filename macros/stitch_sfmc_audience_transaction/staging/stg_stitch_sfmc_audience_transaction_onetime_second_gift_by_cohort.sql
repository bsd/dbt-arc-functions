{% macro create_stg_sfmc_audience_transaction_onetime_second_gift_by_cohort(
    cohort="stg_stitch_sfmc_audience_transaction_with_first_gift_cohort"
) %}

with
    second_1x_transactions as (
        /*
    filter the cohort transaction table to just the second gift,
    only select people who entered as 1x donors
    only look at 1x donations
    */
        select
            person_id,
            transaction_date_day,
            month_diff_str,
            month_diff_int,
            join_month_year_str,
            first_gift_join_source,
            join_gift_size_string,
            first_gift_donor_audience
        from {{ ref(cohort) }}
        where txn_rank = 2 and recurring = false and first_gift_recur_status = false
    )
    
        /*
    number of donors who made their second donation, 
    by their cohort and by their month_diff_str, essentially their activation
    */
        select
            join_month_year_str,
            first_gift_join_source,
            join_gift_size_string,
            first_gift_donor_audience,
            month_diff_str,
            month_diff_int,
            count(distinct person_id) as donors_in_activation
        from second_1x_transactions
        group by 1, 2, 3, 4, 5, 6

        {% endmacro %}

