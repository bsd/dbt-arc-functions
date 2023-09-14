{% macro create_mart_stitch_sfmc_audience_transaction_1x_revenue_by_monthly_cohort(
    reference_name="stg_stitch_sfmc_audience_transaction_monthly_1x_rollup_with_activation"
) %}
    with
        base as (
            select
                join_month_year,
                join_month_year_str,
                donor_audience,
                channel,
                join_gift_size_string,
                activation,
                total_revenue,
                total_donors,
                sum(total_revenue) over (
                    partition by
                        join_month_year_str,
                        donor_audience,
                        channel,
                        join_gift_size_string,
                        join_month_year
                    order by join_month_year
                ) as total_revenue_cumulative_cohort,
            from {{ ref(reference_name) }}
        )

    select
        base.*,
        (
            total_donors / (case when activation = 'Act00' then total_donors end)
        ) as retention_rate,
        (
            total_revenue_cumulative_cohort
            / case when activation = 'Act00' then total_donors end
        ) as value_per_donor
    from base
{% endmacro %}
