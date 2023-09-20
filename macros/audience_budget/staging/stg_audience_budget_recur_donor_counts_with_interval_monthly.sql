{% macro create_stg_audience_budget_recur_donor_counts_with_interval_monthly(
    reference_name='stg_audience_budget_by_day') %}
    select
        last_day(date_day, month) as date_day,
        'monthly' as interval_type,
        donor_audience as donor_audience,
        platform as join_source,
        sum(total_donors_by_day) as recur_total_donor_count_budget,
    from {{ ref(reference_name) }}
    where donor_audience = 'recurring' or donor_audience = 'Monthly'
    group by 1, 2, 3, 4
{% endmacro %}
