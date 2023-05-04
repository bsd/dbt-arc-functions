{% macro create_stg_email_listhealth_year_and_month_concat(
    reference_name="stg_email_listhealth_by_year_and_month"
) %}

with
    base as (
        select
            concat(extract_year, '-', extract_month, '-01') as concat_date,
            max_recipients,
            max_delivered,
            total_hard_bounces,
            total_unsubscribes,
            total_complaints
        from {{ ref(reference_name) }} mart_email
    )

select
    safe_cast(concat_date as date) as date_month,
    safe_cast(max_recipients as int) as max_recipients,
    safe_cast(max_delivered as int) as max_delivered,
    safe_cast(total_unsubscribes as int) as total_unsubscribes,
    safe_cast(total_hard_bounces as int) as total_hard_bounces,
    safe_cast(total_complaints as int) as total_complaints
from base
where concat_date is not null

{% endmacro %}
