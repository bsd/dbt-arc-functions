{% macro create_mart_audience_1x_activation(
    second_gift_by_cohort="stg_stitch_sfmc_audience_transaction_onetime_second_gift_by_cohort",
    rev_by_cohort="stg_stitch_sfmc_audience_transaction_onetime_rev_by_cohort",
    first_gift_explode="stg_stitch_sfmc_audience_transaction_onetime_first_gift_by_cohort"
) %}

    with
        big_join as (
            select
                coalesce(
                    rev_by_cohort.join_month_year_str,
                    first_gift_explode.join_month_year_str,
                    second_gift_by_cohort.join_month_year_str
                ) as join_month_year_str,
                coalesce(
                    rev_by_cohort.first_gift_join_source,
                    first_gift_explode.first_gift_join_source,
                    second_gift_by_cohort.first_gift_join_source
                ) as join_source,
                coalesce(
                    rev_by_cohort.join_gift_size_string,
                    first_gift_explode.join_gift_size_string,
                    second_gift_by_cohort.join_gift_size_string
                ) as join_gift_size,
                coalesce(
                    rev_by_cohort.first_gift_donor_audience,
                    first_gift_explode.first_gift_donor_audience,
                    second_gift_by_cohort.first_gift_donor_audience
                ) as join_donor_audience,
                coalesce(
                    rev_by_cohort.month_diff_int,
                    first_gift_explode.month_diff_int,
                    second_gift_by_cohort.month_diff_int
                ) activation_int,
                first_gift_explode.join_month_year_date,
                rev_by_cohort.total_amount,
                first_gift_explode.donors_in_cohort,
                second_gift_by_cohort.donors_in_activation as donors_activated
            from {{ ref(rev_by_cohort) }} rev_by_cohort
            left join
                {{ ref(second_gift_by_cohort) }} second_gift_by_cohort
                on rev_by_cohort.join_month_year_str
                = second_gift_by_cohort.join_month_year_str
                and rev_by_cohort.first_gift_join_source
                = second_gift_by_cohort.first_gift_join_source
                and rev_by_cohort.join_gift_size_string
                = second_gift_by_cohort.join_gift_size_string
                and rev_by_cohort.first_gift_donor_audience
                = second_gift_by_cohort.first_gift_donor_audience
                and rev_by_cohort.month_diff_int = second_gift_by_cohort.month_diff_int
            full outer join
                {{ ref(first_gift_explode) }} first_gift_explode
                on rev_by_cohort.join_month_year_str
                = first_gift_explode.join_month_year_str
                and rev_by_cohort.first_gift_join_source
                = first_gift_explode.first_gift_join_source
                and rev_by_cohort.join_gift_size_string
                = first_gift_explode.join_gift_size_string
                and rev_by_cohort.first_gift_donor_audience
                = first_gift_explode.first_gift_donor_audience
                and rev_by_cohort.month_diff_int = first_gift_explode.month_diff_int
        )

    select
        *,
        case
            when activation_int < 10
            then 'Act' || lpad(cast(activation_int as string), 2, '0')
            else 'Act' || cast(activation_int as string)
        end as activation_str,
        sum(donors_activated) over (
            partition by
                join_month_year_str, join_source, join_gift_size, join_donor_audience
            order by activation_int asc
        ) as cumulative_donors_activated,
        sum(total_amount) over (
            partition by
                join_month_year_str, join_source, join_gift_size, join_donor_audience
            order by activation_int asc
        ) as cumulative_amount
    from big_join

{% endmacro %}
