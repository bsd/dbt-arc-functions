{% macro create_stg_audience_budget_recur_donor_counts_with_interval_daily(
    reference_name='stg_audience_budget_by_day') %}
    select
        date_day as date_day,
        'daily' as interval_type,
        donor_audience as donor_audience,
        platform as join_source,
        total_donors_by_day as recur_total_donor_count_budget,
    from {{ ref(reference_name) }} as stg_audience_budget_by_day
    where donor_audience = 'recurring'
{% endmacro %}
