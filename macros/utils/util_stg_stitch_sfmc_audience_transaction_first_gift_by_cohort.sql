-- fmt: off
{% macro util_stg_stitch_sfmc_audience_transaction_first_gift_by_cohort(
    recur_status,
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
    {% set boolean_status = True if recur_status == "recurring" else False %}

    with
        first_gift_by_cohort as (
            /*
            group cohort in order to count people in cohort 
            cohort = entered as 1x donors and have other features in common
            make join_month_year_date set to the first date of the month
            */
            select
                join_month_year_str,
                DATE_TRUNC(join_month_year_date, MONTH) as join_month_year_date,
                coalesce(first_gift_join_source, 'Unknown') as first_gift_join_source,
                join_gift_size_string{{ recur_suffix }},
                first_gift_donor_audience,
                count(distinct person_id) as donors_in_cohort
            from {{ ref(first_gift_table) }}
            where first_gift_recur_status = {{ boolean_status }}
            group by 1, 2, 3, 4, 5

        ),

        month_diff_sequence as (
            -- is referenced in first_gift_rollup
            select number as month_diff_int
            from unnest(generate_array(0, 1000)) as number
        )

, cross_join as (
    /*
    use month_diff_sequence to explode the first gift table
    so that we have one row per every activation possibility
    */
    select
        first_gift_by_cohort.join_month_year_str,
        first_gift_by_cohort.join_month_year_date,
        first_gift_by_cohort.first_gift_join_source,
        first_gift_by_cohort.join_gift_size_string{{ recur_suffix }},
        first_gift_by_cohort.first_gift_donor_audience,
        month_diff_sequence.month_diff_int,
        first_gift_by_cohort.donors_in_cohort
    from first_gift_by_cohort
    cross join month_diff_sequence

)

, add_transaction_date as (
    /*
    use month_diff to create a transaction_date that corresponds with the 
    activation or retention month value
    */
    select
    join_month_year_str, 
    join_month_year_date,
    first_gift_join_source,
    join_gift_size_string{{ recur_suffix }},
    first_gift_donor_audience,
    CASE 
        WHEN EXTRACT(DAY FROM join_month_year_date) > EXTRACT(DAY FROM DATE_ADD(LAST_DAY(join_month_year_date, MONTH), INTERVAL month_diff_int MONTH))
        THEN LAST_DAY(DATE_ADD(LAST_DAY(join_month_year_date, MONTH), INTERVAL month_diff_int MONTH))
        ELSE DATE_ADD(DATE_ADD(DATE_TRUNC(join_month_year_date, MONTH), INTERVAL month_diff_int MONTH), INTERVAL EXTRACT(DAY FROM join_month_year_date) - 1 DAY)
    END AS transaction_date,
    month_diff_int,
    donors_in_cohort
    from cross_join

)

/*
filter the whole table to only those transaction values occuring up to today's date
*/
select 
join_month_year_str,
join_month_year_date,
first_gift_join_source,
join_gift_size_string{{ recur_suffix }},
first_gift_donor_audience,
month_diff_int,
donors_in_cohort
from add_transaction_date
where transaction_date <= current_date()


{% endmacro %}
