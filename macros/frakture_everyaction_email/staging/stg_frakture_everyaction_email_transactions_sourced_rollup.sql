{% macro create_stg_frakture_everyaction_email_transactions_sourced_rollup(
    reference_name='stg_frakture_everyaction_email_summary_unioned') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,
    SAFE_CAST(email_summary.revenue AS numeric) AS total_revenue,
    SAFE_CAST(email_summary.transactions AS int) AS total_gifts,
    SAFE_CAST(NULL AS int) AS total_donors,  -- doesn't seem available in Frakture email_summary tables
    SAFE_CAST(email_summary.revenue - email_summary.attributed_recurring_revenue AS numeric) AS one_time_revenue,
    SAFE_CAST(email_summary.transactions - email_summary.attributed_recurring_transactions  AS int) AS one_time_gifts,
    SAFE_CAST(NULL  AS numeric) AS new_monthly_revenue, -- doesn't seem available in Frakture email_summary tables
    SAFE_CAST(NULL  AS int) AS new_monthly_gifts, -- doesn't seem available in Frakture email_summary tables
    SAFE_CAST(email_summary.attributed_recurring_revenue  AS numeric) AS total_monthly_revenue,
    SAFE_CAST(email_summary.attributed_recurring_transactions  AS int) AS total_monthly_gifts
 FROM  {{ ref(reference_name) }} email_summary
{% endmacro %}