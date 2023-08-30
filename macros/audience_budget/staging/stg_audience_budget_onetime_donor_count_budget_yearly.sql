{% macro create_stg_audience_budget_onetime_donor_count_budget_yearly() %}
    with
        sums as (
            select
                last_day(date_day, year) as date_day,
                'yearly' as interval_type,
                donor_audience,
                platform as join_source,
                sum(total_revenue_budget_by_day) as onetime_donor_count_budget,
                sum(loyalty_new_donor_targets_by_day) as onetime_new_donor_count_budget
            from {{ ref("stg_audience_budget_by_day") }}
            where donor_audience != 'recurring'
            group by 1, 2, 3, 4
        )

    select
        date_day,
        interval_type,
        donor_audience,
        join_source,
        onetime_donor_count_budget,
        onetime_new_donor_count_budget,
        sum(onetime_donor_count_budget) over (
            partition by
                donor_audience,
                join_source,
                {{
                    dbt_arc_functions.get_fiscal_year(
                        "date_day", var("fiscal_year_start")
                    )
                }}
            order by date_day
        ) as onetime_donor_count_budget_cumulative,
        sum(onetime_new_donor_count_budget) over (
            partition by
                donor_audience,
                join_source,
                {{
                    dbt_arc_functions.get_fiscal_year(
                        "date_day", var("fiscal_year_start")
                    )
                }}
            order by date_day
        ) as onetime_new_donor_count_cumulative
    from sums

{% endmacro %}