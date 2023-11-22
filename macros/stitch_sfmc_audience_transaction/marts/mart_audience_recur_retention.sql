{% macro create_mart_audience_recur_retention(
    rev_by_cohort="stg_stitch_sfmc_audience_transaction_recur_rev_by_cohort",
    first_gift_explode="stg_stitch_sfmc_audience_transaction_recur_first_gift_by_cohort",
    donors_by_cohort="stg_stitch_sfmc_audience_transaction_recur_donors_by_cohort"
) %}

    with
        big_join as (
            select
                coalesce(
                    rev_by_cohort.join_month_year_str,
                    donors_by_cohort.join_month_year_str,
                    first_gift_explode.join_month_year_str
                ) as join_month_year_str,
                coalesce(
                    rev_by_cohort.first_gift_join_source,
                    donors_by_cohort.first_gift_join_source,
                    first_gift_explode.first_gift_join_source
                ) as join_source,
                coalesce(
                    rev_by_cohort.join_gift_size_string_recur,
                    donors_by_cohort.join_gift_size_string_recur,
                    first_gift_explode.join_gift_size_string_recur
                ) as join_gift_size,
                coalesce(
                    rev_by_cohort.first_gift_donor_audience,
                    donors_by_cohort.first_gift_donor_audience,
                    first_gift_explode.first_gift_donor_audience
                ) as join_donor_audience,
                coalesce(
                    rev_by_cohort.month_diff_int,
                    donors_by_cohort.month_diff_int,
                    first_gift_explode.month_diff_int
                ) retention_int,
                rev_by_cohort.total_amount,
                first_gift_explode.donors_in_cohort,
                donors_by_cohort.unique_donors as donors_retained
            from {{ ref(rev_by_cohort) }} rev_by_cohort
            full outer join
                {{ ref(donors_by_cohort) }} donors_by_cohort
                on rev_by_cohort.join_month_year_str
                = donors_by_cohort.join_month_year_str
                and rev_by_cohort.first_gift_join_source
                = donors_by_cohort.first_gift_join_source
                and rev_by_cohort.join_gift_size_string_recur
                = donors_by_cohort.join_gift_size_string_recur
                and rev_by_cohort.first_gift_donor_audience
                = donors_by_cohort.first_gift_donor_audience
                and rev_by_cohort.month_diff_int = donors_by_cohort.month_diff_int
            full outer join
                {{ ref(first_gift_explode) }} first_gift_explode
                on rev_by_cohort.join_month_year_str
                = first_gift_explode.join_month_year_str
                and rev_by_cohort.first_gift_join_source
                = first_gift_explode.first_gift_join_source
                and rev_by_cohort.join_gift_size_string_recur
                = first_gift_explode.join_gift_size_string_recur
                and rev_by_cohort.first_gift_donor_audience
                = first_gift_explode.first_gift_donor_audience
                and rev_by_cohort.month_diff_int = first_gift_explode.month_diff_int
        )

    select
        *,
        case
            when retention_int < 10
            then 'Ret' || lpad(cast(retention_int as string), 2, '0')
            else 'Ret' || cast(retention_int as string)
        end as retention_str,
        sum(total_amount) over (
            partition by
                join_month_year_str, join_source, join_gift_size, join_donor_audience
            order by retention_int asc
        ) as cumulative_amount
    from big_join

{% endmacro %}
