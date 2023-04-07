{% macro create_stg_frakture_global_message_paidmedia_transactions_sourced_rollup(
    reference_name="stg_frakture_global_message_paidmedia_ad_summary_by_date"
) %}
select
    safe_cast(ad_summary.message_id as string) as message_id,
    safe_cast(ad_summary.date as timestamp) as date_timestamp,
    safe_cast(ad_summary.attributed_revenue as numeric) as total_revenue,
    safe_cast(ad_summary.attributed_transactions as int) as total_gifts,
    safe_cast(ad_summary.origin_person_count as int) as total_donors,
    safe_cast(
        ad_summary.attributed_revenue
        - ad_summary.attributed_recurring_revenue as numeric
    ) as one_time_revenue,
    safe_cast(
        ad_summary.attributed_transactions
        - ad_summary.attributed_recurring_transactions as int
    ) as one_time_gifts,
    safe_cast(
        ad_summary.attributed_initial_recurring_revenue as numeric
    ) as new_monthly_revenue,
    safe_cast(
        ad_summary.attributed_initial_recurring_transactions as int
    ) as new_monthly_gifts,
    safe_cast(
        ad_summary.attributed_recurring_revenue as numeric
    ) as total_monthly_revenue,
    safe_cast(
        ad_summary.attributed_recurring_transactions as int
    ) as total_monthly_gifts,
    safe_cast(ad_summary.goal as string) as objective,
    safe_cast(ad_summary.campaign as string) as campaign,
    safe_cast(ad_summary.campaign_label as string) as campaign_label,
    safe_cast(ad_summary.audience as string) as audience,
    safe_cast(ad_summary.appeal as string) as appeal,
    safe_cast(ad_summary.final_primary_source_code as string) as source_code
from {{ ref(reference_name) }} ad_summary
{% endmacro %}
