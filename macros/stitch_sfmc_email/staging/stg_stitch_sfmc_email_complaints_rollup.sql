{% macro create_stg_stitch_sfmc_email_complaints_rollup(
    reference_name='stg_src_stitch_email_complaint') %}
SELECT SAFE_CAST(job_id AS STRING) AS message_id,
  count(*) as complaints
FROM {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}
