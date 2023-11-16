{% macro create_mart_audience_1x_activation(
    cohort='stg_stitch_sfmc_audience_transaction_with_first_gift_cohort',
    first_gift_table="stg_stitch_sfmc_audience_transaction_first_gift",
) %}


with rev_by_cohort as (
        select
            join_month_year_str,
            coalesce(first_gift_join_source, 'Unknown') as first_gift_join_source,
            join_gift_size_string{{ recur_suffix }},
            first_gift_donor_audience,
            month_diff_int,
            sum(amounts) as total_amount
        from {{ ref(transactions_table) }}
        where first_gift_recur_status = {{ boolean_status }}
        group by 1, 2, 3, 4, 5
    ),


, second_1x_transactions as (
    /*
    filter the cohort transaction table to just the second gift,
    only select people who entered as 1x donors
    only look at 1x donations
    */
    SELECT 
    person_id,
    transaction_date_day,
    month_diff_str,
    month_diff_int,
    join_month_year_str,
    first_gift_join_source,
    join_gift_size_string,
    first_gift_donor_audience
    FROM from {{ref(cohort)}} cohort 
    WHERE txn_rank = 2
    and recurring = False
    and first_gift_recur_status = False
)

-- this table that creates initial donors in cohort value... might drop if I merge these results into the retention/activation mart
, first_gift_by_cohort as (
    /*
    group cohort in order to count people in cohort 
    cohort = entered as 1x donors and have other features in common
    */
    select
        join_month_year_str,
        coalesce(first_gift_join_source, 'Unknown') as first_gift_join_source,
        join_gift_size_string,
        first_gift_donor_audience,
        count(distinct person_id) as donors_in_cohort
        from {{ ref(first_gift_table) }}
        where first_gift_recur_status = False
        group by 1, 2, 3, 4 

)

, second_gift_by_cohort as (
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
    month_diff_int
    count(distinct person_id) as donors_in_activation
    from second_1x_transactions 
    group by 1, 2, 3, 4, 5, 6

)


, month_diff_sequence as (
        select number as month_diff_int from unnest(generate_array(0, 1000)) as number
    )

, first_gift_rollup as (
    /*
    use month_diff_sequence to explode the first gift table
    so that we have one row per every activation possibility
    */
        select
            first_gift_by_cohort.join_month_year_str,
            first_gift_by_cohort.first_gift_join_source,
            first_gift_by_cohort.join_gift_size_string,
            first_gift_by_cohort.first_gift_donor_audience,
            month_diff_sequence.month_diff_int,
            first_gift_by_cohort.donors_in_cohort
        from first_gift_by_cohort
        cross join month_diff_sequence

    )

, big_join as (
    select
    coalesce(rev_by_cohort.join_month_year_str, first_gift_rollup.join_month_year_str, second_gift_by_cohort.join_month_year_str) as join_month_year_str,
    coalesce(rev_by_cohort.first_gift_join_source, first_gift_rollup.first_gift_join_source, second_gift_by_cohort.first_gift_join_source) as join_source,
    coalesce(rev_by_cohort.join_gift_size_string, first_gift_rollup.join_gift_size_string, second_gift_by_cohort.join_gift_size_string) as join_gift_size,
    coalesce(rev_by_cohort.first_gift_donor_audience, first_gift_rollup.first_gift_donor_audience, second_gift_by_cohort.first_gift_donor_audience) as join_donor_audience,
    coalesce(rev_by_cohort.month_diff_int, first_gift_rollup.month_diff_int, second_gift_by_cohort.month_diff_int) as month_diff_int,
    rev_by_cohort.total_amount,
    first_gift_rollup.donors_in_cohort,
    second_gift_by_cohort.donors_in_activation, as donors_in_activation
    from first_gift_rollup 
    full outer join  second_gift_by_cohort
    on second_gift_by_cohort.join_month_year_str = first_gift_rollup.join_month_year_str
    and second_gift_by_cohort.first_gift_join_source
    = first_gift_rollup.first_gift_join_source
    and second_gift_by_cohort.join_gift_size_string
    = first_gift_rollup.join_gift_size_string
    and second_gift_by_cohort.first_gift_donor_audience
    = first_gift_rollup.first_gift_donor_audience
    and second_gift_by_cohort.month_diff_int = first_gift_rollup.month_diff_int
)

select
    *,
    sum(donors_in_activation) over (
        partition by
            join_month_year_str, join_source, join_gift_size, join_donor_audience
        order by month_diff_int asc
    ) as cumulative_donors_in_activation,
     sum(total_amount) over (
        partition by
            join_month_year_str, join_source, join_gift_size, join_donor_audience
        order by month_diff_int asc
    ) as cumulative_amount
from big_join



{% endmacro %}


