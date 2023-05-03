{% macro create_stg_email_listhealth_with_prev_delivered(
    reference_name="stg_email_listhealth_by_year_and_month_concat"
) %}
select
    date_month,
    max_recipients,
    max_delivered,
    lag(max_delivered) over (order by date_month asc) as delivered_prev_month,
    total_unsubscribes,
    total_hard_bounces,
    total_complaints
from {{ ref(reference_name) }} mart_email

{% endmacro %}
