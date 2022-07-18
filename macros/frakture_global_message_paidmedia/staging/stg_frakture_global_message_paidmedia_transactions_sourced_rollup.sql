{% macro create_stg_frakture_global_message_paidmedia_transactions_sourced_rollup(
    reference_name='stg_frakture_global_message_paidmedia_ad_summary_by_date') %}
SELECT SAFE_CAST(ad_summary.message_id AS STRING) AS message_id,    
    MIN(SAFE_CAST(ad_summary.date AS TIMESTAMP)) AS date_timestamp,
    SUM(SAFE_CAST(ad_summary.attributed_revenue AS numeric)) AS total_revenue,
    SUM(SAFE_CAST(ad_summary.attributed_transactions AS int)) AS total_gifts,
    SUM(SAFE_CAST(ad_summary.origin_person_count AS int)) AS total_donors,  -- doesn't seem available in Frakture ad_summary tables
    SUM(SAFE_CAST(ad_summary.attributed_revenue - ad_summary.attributed_recurring_revenue AS numeric)) AS one_time_revenue,
    SUM(SAFE_CAST(ad_summary.attributed_transactions - ad_summary.attributed_recurring_transactions  AS int)) AS one_time_gifts,
    SUM(SAFE_CAST(NULL  AS numeric)) AS new_monthly_revenue, -- doesn't seem available in Frakture ad_summary tables
    SUM(SAFE_CAST(NULL  AS int)) AS new_monthly_gifts, -- doesn't seem available in Frakture ad_summary tables
    SUM(SAFE_CAST(ad_summary.attributed_recurring_revenue  AS numeric)) AS total_monthly_revenue,
    SUM(SAFE_CAST(ad_summary.attributed_recurring_transactions  AS int)) AS total_monthly_gifts,
    MIN(SAFE_CAST(ad_summary.campaign  AS STRING)) AS campaign,
    MIN(SAFE_CAST(ad_summary.campaign_label  AS STRING)) AS campaign_label,
    MIN(SAFE_CAST(ad_summary.audience  AS STRING)) AS audience,
    MIN(SAFE_CAST(ad_summary.appeal AS STRING)) AS appeal
 FROM  {{ ref(reference_name) }} ad_summary
 GROUP BY 1
{% endmacro %}