{% macro create_mart_email_listhealth_by_month(
    reference_name="stg_email_listhealth_with_prev_delivered"
) %}
    select
        date_month,
        max_recipients,
        max_delivered,
        cast(delivered_prev_month as int) as delivered_prev_month,
        cast(max_delivered as int)
        - cast(delivered_prev_month as int) as diff_delivered_prev_month,
        total_unsubscribes,
        total_hard_bounces,
        total_complaints
    from {{ ref(reference_name) }} mart_email

{% endmacro %}
