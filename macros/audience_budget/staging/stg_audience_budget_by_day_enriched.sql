{% macro create_stg_audience_budget_by_day_enriched(
    budget_by_day="stg_audience_budget_by_day"
) %}

    select
        platform,
        date_spine_year,
        date_spine_month,
        date_spine_day,
        {{ dbt_arc_functions.get_fiscal_year("date_day", var("fiscal_year_start")) }}
        as fiscal_year,
        date(date_day) as date_day,
        total_revenue_budget_by_day,
        loyalty_new_donor_targets_by_day,
        loyalty_unknown_donor_targets_by_day,
        loyalty_retained_donor_targets_by_day,
        loyalty_retained_three_donor_targets_by_day,
        loyalty_reinstated_donor_targets_by_day,
        total_donors_by_day,
        initcap(donor_audience) as donor_audience  -- UUSA custom audience naming code

    from {{ ref(budget_by_day) }}

{% endmacro %}
