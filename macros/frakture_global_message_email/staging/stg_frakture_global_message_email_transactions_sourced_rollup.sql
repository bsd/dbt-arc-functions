{% macro create_stg_frakture_global_message_email_transactions_sourced_rollup(
    reference_name='stg_frakture_global_message_email_summary') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,   
    SUM(SAFE_CAST(attributed_revenue AS numeric)) AS total_revenue,
    SUM(SAFE_CAST(attributed_transactions AS int)) AS total_gifts,
    SUM(SAFE_CAST(origin_person_count AS int)) AS total_donors,  -- doesn't seem available in Frakture ad_summary tables
    SUM(SAFE_CAST(attributed_revenue - attributed_recurring_revenue AS numeric)) AS one_time_revenue,
    SUM(SAFE_CAST(attributed_transactions - attributed_recurring_transactions  AS int)) AS one_time_gifts,
    SUM(SAFE_CAST(attributed_initial_recurring_revenue  AS numeric)) AS new_monthly_revenue,
    SUM(SAFE_CAST(attributed_initial_recurring_transactions  AS int)) AS new_monthly_gifts, 
    SUM(SAFE_CAST(attributed_recurring_revenue  AS numeric)) AS total_monthly_revenue,
    SUM(SAFE_CAST(attributed_recurring_transactions  AS int)) AS total_monthly_gifts
 FROM  {{ ref(reference_name) }} 
 GROUP BY 1
{% endmacro %}
