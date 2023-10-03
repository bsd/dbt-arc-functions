{% macro create_mart_stitch_sfmc_audience_transaction_1x_revenue_by_monthly_cohort(
    reference_name="stg_stitch_sfmc_audience_transaction_monthly_1x_rollup_with_activation"
) %}

    with
        activation_donors_base as (
            select
                join_month_year_str,
                donor_audience,
                channel,
                join_gift_size_string,
                max(
                    case when activation = 'Act00' then total_donors end
                ) as activation_donors
            from {{ ref(reference_name) }}
            group by
                join_month_year_str, donor_audience, channel, join_gift_size_string
        ),
        base as (
            select
                a.join_month_year,
                a.join_month_year_str,
                a.transaction_month_year_date,
                a.transaction_month_year_str,
                a.donor_audience,
                a.channel,
                a.join_gift_size_string,
                a.activation,
                a.total_revenue,
                a.total_donors,
                sum(a.total_revenue) over (
                    partition by
                        join_month_year_str,
                        donor_audience,
                        join_source,
                        join_gift_size_string
                    order by activation
                ) as total_revenue_cumulative_cohort,
                b.activation_donors as activation_donors
            from {{ ref(reference_name) }} a
            join
                activation_donors_base b
                on a.join_month_year_str = b.join_month_year_str
                and a.donor_audience = b.donor_audience
                and a.channel = b.channel
                and a.join_gift_size_string = b.join_gift_size_string
        )

    select
        join_month_year,
        join_month_year_str,
        transaction_month_year_date,
        transaction_month_year_str,
        donor_audience,
        channel,
        join_gift_size_string,
        activation,
        total_revenue,
        total_donors,
        total_revenue_cumulative_cohort,
        case
            when activation_donors = 0 then 0 else total_donors / activation_donors
        end as retention_rate,
        case
            when activation_donors = 0
            then 0
            else total_revenue_cumulative_cohort / activation_donors
        end as value_per_donor
    from base

{% endmacro %}
