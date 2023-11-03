{% macro create_mart_stitch_sfmc_audience_transaction_1x_revenue_by_monthly_cohort(
    reference_name="stg_stitch_sfmc_audience_transaction_monthly_1x_rollup_with_activation"
) %}

/*

This query calculates the first_activation_donor_count for each monthly cohort,
which is the number of donors that the cohort had in their first activation month.

 To do this, the query first joins two tables:

 * The `monthly_1x_rollup` table, which contains aggregated data for each month,
 donor audience, channel, and join gift size.
 * The `activation_donors_base` table, which contains the number of donors
 that each cohort had in their first activation month.

 The query then calculates the cumulative revenue for each cohort, which is the
 total revenue that the cohort has generated since their first activation month.

 Finally, the query calculates the first_activation_donor_count for each cohort by
 coalescing the number of donors from the `activation_donors_base` table with
 the maximum number of donors from the `monthly_1x_rollup` table for the cohort's
 first activation month. This ensures that the first_activation_donor_count is always
 populated, even if there is no record in the `activation_donors_base` table for
 the cohort's first activation month.

*/

with
    activation_donors_base as (
        select
            join_month_year_str,
            donor_audience,
            channel,
            join_gift_size_string,
            sum(total_donors) as activation_donors
        from {{ ref(reference_name) }} monthly_1x_rollup
        where activation = 'Act00'
        group by 1, 2, 3, 4
    ),
    base as (
        select
            monthly_1x_rollup.join_month_year,
            monthly_1x_rollup.join_month_year_str,
            monthly_1x_rollup.transaction_month_year_date,
            monthly_1x_rollup.transaction_month_year_str,
            monthly_1x_rollup.donor_audience,
            monthly_1x_rollup.channel,
            monthly_1x_rollup.join_gift_size_string,
            monthly_1x_rollup.activation,
            monthly_1x_rollup.total_revenue,
            monthly_1x_rollup.total_donors,
            sum(monthly_1x_rollup.total_revenue) over (
                partition by
                    monthly_1x_rollup.join_month_year_str,
                    monthly_1x_rollup.donor_audience,
                    monthly_1x_rollup.channel,
                    monthly_1x_rollup.join_gift_size_string
                order by monthly_1x_rollup.activation
            ) as total_revenue_cumulative_cohort,
            activation_donors_base.activation_donors 
        from {{ ref(reference_name) }} monthly_1x_rollup
        left join
            activation_donors_base
            on monthly_1x_rollup.join_month_year_str = activation_donors_base.join_month_year_str
            and monthly_1x_rollup.donor_audience = activation_donors_base.donor_audience
            and monthly_1x_rollup.channel = activation_donors_base.channel
            and monthly_1x_rollup.join_gift_size_string = activation_donors_base.join_gift_size_string
    )

select
    join_month_year,
    join_month_year_str,
    transaction_month_year_date,
    transaction_month_year_str,
    donor_audience,
    channel,
    join_gift_size_string,
    case
        when join_gift_size_string = "0-25"
        then 1
        when join_gift_size_string = "26-100"
        then 2
        when join_gift_size_string = "101-250"
        then 3
        when join_gift_size_string = "251-500"
        then 4
        when join_gift_size_string = "501-1000"
        then 5
        when join_gift_size_string = "1001-10000"
        then 6
        when join_gift_size_string = "10000+"
        then 7
    end join_amount_string_sort,
    activation,
    total_revenue,
    total_donors,
    total_revenue_cumulative_cohort,
    COALESCE(MAX(COALESCE(activation_donors, 0)) OVER (), 0) as first_activation_donor_count
from base

{% endmacro %}
