{% macro create_stg_frakture_global_message_paidmedia_transactions_sourced_rollup(
    reference_name='stg_frakture_global_message_paidmedia_ad_summary_by_date') %}
SELECT SAFE_CAST(ad_summary.message_id AS STRING) AS message_id,
    SAFE_CAST(ad_summary.date AS TIMESTAMP) AS date_timestamp,
    SAFE_CAST(ad_summary.attributed_revenue AS numeric) AS total_revenue,
    SAFE_CAST(ad_summary.attributed_transactions AS int) AS total_gifts,
    SAFE_CAST(ad_summary.origin_person_count AS int) AS total_donors, 
    SAFE_CAST(ad_summary.attributed_revenue - ad_summary.attributed_recurring_revenue AS numeric) AS one_time_revenue,
    SAFE_CAST(ad_summary.attributed_transactions - ad_summary.attributed_recurring_transactions  AS int) AS one_time_gifts,
    SAFE_CAST(ad_summary.attributed_initial_recurring_revenue  AS numeric) AS new_monthly_revenue,
    SAFE_CAST(ad_summary.attributed_initial_recurring_transactions  AS int) AS new_monthly_gifts,
    SAFE_CAST(ad_summary.attributed_recurring_revenue  AS numeric) AS total_monthly_revenue,
    SAFE_CAST(ad_summary.attributed_recurring_transactions  AS int) AS total_monthly_gifts,
    SAFE_CAST(ad_summary.goal AS STRING) AS objective,
    SAFE_CAST(ad_summary.campaign  AS STRING) AS campaign,
    SAFE_CAST(ad_summary.campaign_label  AS STRING) AS campaign_label,
    SAFE_CAST(ad_summary.audience  AS STRING) AS audience,
    SAFE_CAST(ad_summary.appeal AS STRING) AS appeal,
    SAFE_CAST(ad_summary.final_primary_source_code as STRING) as source_code
 FROM  {{ ref(reference_name) }} ad_summary
{% endmacro %}