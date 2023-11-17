{% macro util_mart_audience_retention_activation(
    recur_status,
    first_gift_table="stg_stitch_sfmc_audience_transaction_first_gift",
    transactions_table="stg_stitch_sfmc_audience_transaction_with_first_gift_cohort"
) %}

{% if recur_status not in ["recurring", "onetime"] %}
{{
    exceptions.raise_compiler_error(
        "'recur_status' argument to util_mart_audience_retention_activation must be 'recurring' or 'onetime', got "
        ~ recur_status
    )
}}
{% endif %}

{% set recur_suffix = "_recur" if recur_status == "recurring" else "" %}
{% set boolean_status = True if recur_status == "recurring" else False %}
{% set ret_or_act = "Ret" if recur_status == "recurring" else "Act" %}
{% set retention_or_activation = (
    "retention" if recur_status == "recurring" else "activation"
) %}
{% set donors_in_cohort = (
    "stg_stitch_sfmc_audience_transaction_first_gift_recur_rollup"
    if recur_status == "recurring"
    else "stg_stitch_sfmc_audience_transaction_first_gift_1x_rollup"
) %}

with
    first_gift_by_cohort as (
        select
            join_month_year_str,
            coalesce(first_gift_join_source, 'Unknown') as first_gift_join_source,
            join_gift_size_string{{ recur_suffix }},
            first_gift_donor_audience,
            count(distinct person_id) as donors_in_cohort
        from {{ ref(first_gift_table) }}
        where first_gift_recur_status = {{ boolean_status }}
        group by 1, 2, 3, 4

    ),
    rev_by_cohort as (
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

    month_diff_sequence as (
        select number as month_diff_int from unnest(generate_array(0, 1000)) as number
    ),
    first_gift_rollup as (
        select
            first_gift_by_cohort.join_month_year_str,
            first_gift_by_cohort.first_gift_join_source,
            first_gift_by_cohort.join_gift_size_string{{ recur_suffix }},
            first_gift_by_cohort.first_gift_donor_audience,
            month_diff_sequence.month_diff_int,
            first_gift_by_cohort.donors_in_cohort
        from first_gift_by_cohort
        cross join month_diff_sequence

    ),
    join_cohorts as (
        select
            coalesce(
                rev_by_cohort.join_month_year_str, first_gift_rollup.join_month_year_str
            ) as join_month_year_str,
            coalesce(
                rev_by_cohort.first_gift_join_source,
                first_gift_rollup.first_gift_join_source
            ) as join_source,
            coalesce(
                rev_by_cohort.join_gift_size_string{{ recur_suffix }},
                first_gift_rollup.join_gift_size_string{{ recur_suffix }}
            ) as join_gift_size,
            coalesce(
                rev_by_cohort.first_gift_donor_audience,
                first_gift_rollup.first_gift_donor_audience
            ) as join_donor_audience,
            case
                when
                    coalesce(
                        rev_by_cohort.month_diff_int, first_gift_rollup.month_diff_int
                    )
                    < 100
                then
                    '{{ ret_or_act }}' || lpad(
                        cast(
                            coalesce(
                                rev_by_cohort.month_diff_int,
                                first_gift_rollup.month_diff_int
                            ) as string
                        ),
                        2,
                        '0'
                    )
                when
                    coalesce(
                        rev_by_cohort.month_diff_int, first_gift_rollup.month_diff_int
                    )
                    between 100 and 999
                then
                    '{{ ret_or_act }}' || lpad(
                        cast(
                            coalesce(
                                rev_by_cohort.month_diff_int,
                                first_gift_rollup.month_diff_int
                            ) as string
                        ),
                        3,
                        '0'
                    )
                when
                    coalesce(
                        rev_by_cohort.month_diff_int, first_gift_rollup.month_diff_int
                    )
                    between 1000 and 9999
                then
                    '{{ ret_or_act }}' || lpad(
                        cast(
                            coalesce(
                                rev_by_cohort.month_diff_int,
                                first_gift_rollup.month_diff_int
                            ) as string
                        ),
                        4,
                        '0'
                    )
                else
                    '{{ ret_or_act }}' || lpad(
                        cast(
                            coalesce(
                                rev_by_cohort.month_diff_int,
                                first_gift_rollup.month_diff_int
                            ) as string
                        ),
                        5,
                        '0'
                    )
            end as {{ retention_or_activation }}_str,
            coalesce(
                rev_by_cohort.month_diff_int, first_gift_rollup.month_diff_int
            ) as month_diff_int,
            rev_by_cohort.total_amount as total_amount,
            first_gift_rollup.donors_in_cohort
        from rev_by_cohort
        full outer join
            first_gift_rollup
            on rev_by_cohort.join_month_year_str = first_gift_rollup.join_month_year_str
            and rev_by_cohort.first_gift_join_source
            = first_gift_rollup.first_gift_join_source
            and rev_by_cohort.join_gift_size_string{{ recur_suffix }}
            = first_gift_rollup.join_gift_size_string{{ recur_suffix }}
            and rev_by_cohort.first_gift_donor_audience
            = first_gift_rollup.first_gift_donor_audience
            and rev_by_cohort.month_diff_int = first_gift_rollup.month_diff_int
    )

select
    *,
    sum(total_amount) over (
        partition by
            join_month_year_str, join_source, join_gift_size, join_donor_audience
        order by month_diff_int asc
    ) as cumulative_amount
from join_cohorts

{% endmacro %}