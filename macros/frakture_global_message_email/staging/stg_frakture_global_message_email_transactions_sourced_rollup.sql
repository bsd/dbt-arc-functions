{% macro create_stg_frakture_global_message_email_transactions_sourced_rollup(
    reference_name='stg_frakture_global_message_email_summary_by_date') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,   
SAFE_CAST(publish_date AS TIMESTAMP) AS date_timestamp, 
    SAFE_CAST(attributed_revenue AS numeric) AS total_revenue,
    SAFE_CAST(attributed_transactions AS int) AS total_gifts,
    SAFE_CAST(origin_person_count AS int) AS total_donors,  -- doesn't seem available in Frakture ad_summary tables
    SAFE_CAST(attributed_revenue - attributed_recurring_revenue AS numeric) AS one_time_revenue,
    SAFE_CAST(attributed_transactions - attributed_recurring_transactions  AS int) AS one_time_gifts,
    SAFE_CAST(NULL  AS numeric) AS new_monthly_revenue, -- doesn't seem available in Frakture ad_summary tables
    SAFE_CAST(NULL  AS int) AS new_monthly_gifts, -- doesn't seem available in Frakture ad_summary tables
    SAFE_CAST(attributed_recurring_revenue  AS numeric) AS total_monthly_revenue,
    SAFE_CAST(attributed_recurring_transactions  AS int) AS total_monthly_gifts
 FROM  {{ ref(reference_name) }} 
{% endmacro %}
