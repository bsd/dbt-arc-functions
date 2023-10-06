{% macro create_stg_audience_budget_recur_donor_counts_with_interval_combined(
    reference_0_name="stg_audience_budget_recur_donor_counts_with_interval_daily",
    reference_1_name="stg_audience_budget_recur_donor_counts_with_interval_monthly",
    reference_2_name="stg_audience_budget_recur_donor_counts_with_interval_yearly"
) %}
    select *
    from {{ ref(reference_0_name) }}
    union all
    select *
    from {{ ref(reference_1_name) }}
    union all
    select *
    from {{ ref(reference_2_name) }}

{% endmacro %}
