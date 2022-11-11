{% macro create_stg_frakture_actionkit_email_transactions_sourced_rollup(
    email_summary='stg_frakture_actionkit_email_summary_unioned',
    transactions='stg_frakture_actionkit_transactions_summary_unioned'
    ) %}

WITH GROUPED as (
SELECT 
    email_summary.message_id AS message_id,
    SUM(SAFE_CAST(transactions.amount AS numeric)) AS total_revenue,
    COUNT(DISTINCT transactions.remote_transaction_id) AS total_gifts,
    COUNT(DISTINCT transactions.remote_person_id) AS total_donors, 
    SUM(CASE WHEN transactions.is_recurring = '0' then transactions.amount end) AS one_time_revenue,
    COUNT(CASE WHEN transactions.is_recurring = '0' then transactions.remote_transaction_id end) AS one_time_gifts,
    SUM(CASE WHEN transactions.recurring_number = 1 then transactions.amount end) AS new_monthly_revenue,
    COUNT(CASE WHEN transactions.recurring_number = 1 then transactions.remote_transaction_id end) AS new_monthly_gifts,
    SUM(CASE WHEN transactions.is_recurring = '1' then transactions.amount end) AS total_monthly_revenue,
    COUNT(CASE WHEN transactions.is_recurring = '1' then transactions.remote_transaction_id end) AS total_monthly_gifts
 FROM  {{ ref(email_summary) }} email_summary
 FULL OUTER JOIN {{ ref(transactions) }} transactions 
 ON CAST(email_summary.remote_id as STRING) = CAST(transactions.remote_message_id as STRING)
 GROUP BY 1 )

SELECT 
    SAFE_CAST(message_id as STRING) AS message_id,
    SAFE_CAST(total_revenue as NUMERIC) AS total_revenue,
    SAFE_CAST(total_gifts as INT) AS total_gifts,
    SAFE_CAST(total_donors as INT) AS total_donors,
    SAFE_CAST(one_time_gifts as INT) AS one_time_gifts,
    SAFE_CAST(one_time_revenue as NUMERIC) AS one_time_revenue,
    SAFE_CAST(new_monthly_revenue  AS numeric) AS new_monthly_revenue,
    SAFE_CAST(new_monthly_gifts  AS int) AS new_monthly_gifts,
    SAFE_CAST(total_monthly_revenue AS numeric) AS total_monthly_revenue,
    SAFE_CAST(total_monthly_gifts  AS int) AS total_monthly_gifts
FROM GROUPED

{% endmacro %}