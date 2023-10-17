{% macro create_stg_audience_budget_recur_donor_counts_with_interval_yearly(
    reference_name="stg_audience_budget_by_day"
) %}
    select
        last_day(date_day, year) as date_day,
        'yearly' as interval_type,
        donor_audience as donor_audience,
        platform as join_source,
        sum(total_donors_by_day) as recur_total_donor_count_budget,
    from {{ ref(reference_name) }}
    where lower(donor_audience) = 'recurring' or lower(donor_audience) = 'monthly'
    group by 1, 2, 3, 4
{% endmacro %}
