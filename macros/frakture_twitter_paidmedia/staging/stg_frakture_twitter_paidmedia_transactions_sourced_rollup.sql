{% macro create_stg_frakture_twitter_paidmedia_transactions_sourced_rollup(
    reference_name='stg_frakture_twitter_paidmedia_ad_summary_by_date_unioned') %}
SELECT SAFE_CAST(ad_summary.message_id AS STRING) AS message_id,    SAFE_CAST(ad_summary.date AS TIMESTAMP) AS date_timestamp,
    SAFE_CAST(ad_summary.attributed_revenue AS numeric) AS total_revenue,
    SAFE_CAST(ad_summary.attributed_transactions AS int) AS total_gifts,
    SAFE_CAST(ad_summary.origin_person_count AS int) AS total_donors,  -- doesn't seem available in Frakture ad_summary_pivot tables
    SAFE_CAST(ad_summary.attributed_revenue - ad_summary.attributed_recurring_revenue AS numeric) AS one_time_revenue,
    SAFE_CAST(ad_summary.attributed_transactions - ad_summary.attributed_recurring_transactions  AS int) AS one_time_gifts,
    SAFE_CAST(NULL  AS numeric) AS new_monthly_revenue, -- doesn't seem available in Frakture ad_summary_pivot tables
    SAFE_CAST(NULL  AS int) AS new_monthly_gifts, -- doesn't seem available in Frakture ad_summary_pivot tables
    SAFE_CAST(ad_summary.attributed_recurring_revenue  AS numeric) AS total_monthly_revenue,
    SAFE_CAST(ad_summary.attributed_recurring_transactions  AS int) AS total_monthly_gifts
 FROM  {{ ref(reference_name) }} ad_summary
{% endmacro %}