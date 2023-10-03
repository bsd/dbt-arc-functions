{% macro create_mart_stitch_sfmc_audience_transaction_recur_revenue_by_monthly_cohort(
    reference_name="stg_stitch_sfmc_audience_transaction_monthly_recurring_rollup_with_activation"
) %}

    with
        activation_donors_base as (
            select
                join_month_year_str,
                donor_audience,
                join_source,
                join_gift_size_string,
                max(
                    case when activation = 'Act00' then total_donors end
                ) as activation_donors
            from {{ ref(reference_name) }}
            group by
                join_month_year_str, donor_audience, join_source, join_gift_size_string
        ),
        base as (
            select
                a.join_month_year,
                a.join_month_year_str,
                a.transaction_month_year_date,
                a.transaction_month_year_str,
                a.donor_audience,
                a.join_source,
                a.join_gift_size_string,
                a.activation,
                a.total_revenue,
                a.total_donors,
                sum(a.total_revenue) over (
                    partition by
                        a.join_month_year_str,
                        a.donor_audience,
                        a.join_source,
                        a.join_gift_size_string
                    order by a.activation
                ) as total_revenue_cumulative_cohort,
                b.activation_donors as activation_donors
            from {{ ref(reference_name) }} a
            join
                activation_donors_base b
                on a.join_month_year_str = b.join_month_year_str
                and a.donor_audience = b.donor_audience
                and a.join_source = b.join_source
                and a.join_gift_size_string = b.join_gift_size_string
        )

    select
        join_month_year,
        join_month_year_str,
        transaction_month_year_date,
        transaction_month_year_str,
        donor_audience,
        join_source,
        join_gift_size_string, 
        CASE
    WHEN REGEXP_CONTAINS(join_gift_size_string,"0[-]25") THEN 1
    WHEN REGEXP_CONTAINS(join_gift_size_string,"26[-]100") THEN 2
    WHEN REGEXP_CONTAINS(join_gift_size_string,"101[-]250") THEN 3
    WHEN REGEXP_CONTAINS(join_gift_size_string,"251[-]500") THEN 4
    WHEN REGEXP_CONTAINS(join_gift_size_string,"501[-]1000") THEN 5
    WHEN REGEXP_CONTAINS(join_gift_size_string,"1001[-]10000") THEN 6
    WHEN REGEXP_CONTAINS(join_gift_size_string,"10000+") THEN 7
END join_amount_string_sort, -- change this one the join amount has been adjusted for recur
        activation,
        total_revenue,
        total_donors,
        total_revenue_cumulative_cohort,
        activation_donors as first_activation_donor_count,
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
