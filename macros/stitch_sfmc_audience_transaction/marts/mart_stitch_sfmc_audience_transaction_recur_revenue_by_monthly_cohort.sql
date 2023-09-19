{% macro create_mart_stitch_sfmc_audience_transaction_recur_revenue_by_monthly_cohort(
    reference_name="stg_stitch_sfmc_audience_transaction_monthly_recurring_rollup_with_activation"
) %}
    with
        base as (
            select
                join_month_year,
                join_month_year_str,
                transaction_month_year_date,
                transaction_month_year_str,
                donor_audience,
                join_source,
                join_gift_size_string,
                activation,
                total_revenue,
                total_donors,
                sum(total_revenue) over (
                    partition by
                        join_month_year_str,
                        donor_audience,
                        join_source,
                        join_gift_size_string
                    order by transaction_month_year_date
                ) as total_revenue_cumulative_cohort, -- this isn't correct
                case when activation = 'Act00' then total_donors end as activation_donors
            from {{ ref(reference_name) }}
        )

    select
    join_month_year,
    join_month_year_str,
    transaction_month_year_date,
    transaction_month_year_str,
    donor_audience,
    join_source,
    join_gift_size_string,
    activation,
    total_revenue,
    total_donors,
    total_revenue_cumulative_cohort,
    case when activation_donors = 0 then 0 else total_donors / activation_donors end as retention_rate,
    case when activation_donors = 0 then 0 else total_revenue_cumulative_cohort / activation_donors end as value_per_donor
    from base

{% endmacro %}
