{% macro create_stg_stitch_sfmc_email_job(
    reference_name='stg_src_stitch_sfmc_email_job') %}
SELECT 
   DISTINCT SAFE_CAST(job_id AS STRING) AS message_id,
    SAFE_CAST(email_id as STRING) AS email_id,
    SAFE_CAST(from_name AS STRING) AS from_name,
    SAFE_CAST(from_email AS STRING) AS from_email,
    SAFE_CAST(coalesce(sched_dt,pickup_dt) AS TIMESTAMP) AS best_guess_timestamp,
    SAFE_CAST(sched_dt AS TIMESTAMP) AS scheduled_timestamp,
    SAFE_CAST(pickup_dt AS TIMESTAMP) AS pickup_timestamp,
    SAFE_CAST(delivered_dt AS TIMESTAMP) AS delivered_timestamp,
    SAFE_CAST(email_name AS STRING) AS email_name,
    SAFE_CAST(email_subject AS STRING) AS email_subject,
    SAFE_CAST(category as STRING) AS category,
    SAFE_CAST(NULL AS STRING) AS source_code
FROM {{ ref(reference_name) }}
{% endmacro %}
