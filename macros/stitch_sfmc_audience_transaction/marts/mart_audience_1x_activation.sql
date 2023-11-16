{% macro create_mart_audience_1x_activation(
    second_gift_by_cohort='stg_stitch_sfmc_audience_transaction_onetime_second_gift_by_cohort',
    rev_by_cohort='stg_stitch_sfmc_audience_transaction_onetime_rev_by_cohort',
    first_gift_explode='stg_stitch_sfmc_audience_transaction_onetime_first_gift_by_cohort'
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
            ) as month_diff_int,
            rev_by_cohort.total_amount,
            first_gift_explode.donors_in_cohort,
            second_gift_by_cohort.donors_in_activation 
        from {{ref(rev_by_cohort)}} rev_by_cohort
        left join
            {{ref(second_gift_by_cohort)}}   second_gift_by_cohort
            on rev_by_cohort.join_month_year_str
            = second_gift_by_cohort.join_month_year_str
            and rev_by_cohort.first_gift_join_source
            = second_gift_by_cohort.first_gift_join_source
            and rev_by_cohort.join_gift_size_string
            = second_gift_by_cohort.join_gift_size_string
            and rev_by_cohort.first_gift_donor_audience
            = second_gift_by_cohort.first_gift_donor_audience
            and rev_by_cohort.month_diff_int
            = second_gift_by_cohort.month_diff_int
        full outer join
            {{ref(first_gift_explode)}} first_gift_explode
            on rev_by_cohort.join_month_year_str = first_gift_explode.join_month_year_str
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
        when month_diff_int < 100
        then 'Act' || lpad(cast(month_diff_int as string), 2, '0')
        when month_diff_int between 100 and 999
        then 'Act' || lpad(cast(month_diff_int as string), 3, '0')
        when month_diff_int between 1000 and 9999
        then 'Act' || lpad(cast(month_diff_int as string), 4, '0')
        else '{{ ret_or_act }}' || lpad(cast(month_diff_int as string), 5, '0')
    end as {{ retention_or_activation }}_str,
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
