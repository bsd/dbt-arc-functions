{% macro create_stg_email_listhealth_by_year_and_month(
    reference_name="mart_email_performance_with_revenue"
) %}
    select
        extract(year from best_guess_timestamp) as extract_year,
        extract(month from best_guess_timestamp) as extract_month,
        max(recipients) as max_recipients,
        max(recipients - total_bounces) as max_delivered,
        sum(unsubscribes) as total_unsubscribes,
        sum(hard_bounces) as total_hard_bounces,
        sum(complaints) as total_complaints
    from {{ ref(reference_name) }} mart_email
    group by 1, 2

{% endmacro %}
