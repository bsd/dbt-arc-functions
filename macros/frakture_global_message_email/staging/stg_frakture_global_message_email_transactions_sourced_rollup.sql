{% macro create_stg_frakture_global_message_email_transactions_sourced_rollup(
    reference_name="stg_frakture_global_message_email_summary"
) %}
select
    safe_cast(message_id as string) as message_id,
    sum(safe_cast(attributed_revenue as numeric)) as total_revenue,
    sum(safe_cast(attributed_transactions as int)) as total_gifts,
    sum(safe_cast(origin_person_count as int)) as total_donors,  -- doesn't seem available in Frakture ad_summary tables
    sum(
        safe_cast(attributed_revenue - attributed_recurring_revenue as numeric)
    ) as one_time_revenue,
    sum(
        safe_cast(attributed_transactions - attributed_recurring_transactions as int)
    ) as one_time_gifts,
    sum(
        safe_cast(attributed_initial_recurring_revenue as numeric)
    ) as new_monthly_revenue,
    sum(
        safe_cast(attributed_initial_recurring_transactions as int)
    ) as new_monthly_gifts,
    sum(safe_cast(attributed_recurring_revenue as numeric)) as total_monthly_revenue,
    sum(safe_cast(attributed_recurring_transactions as int)) as total_monthly_gifts
from {{ ref(reference_name) }}
group by 1
{% endmacro %}
