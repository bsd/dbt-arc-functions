{% macro create_stg_stitch_sfmc_email_unsubscribes_rollup(
    reference_name='stg_src_stitch_email_unsubscribe') %}
 SELECT SAFE_CAST(job_id AS STRING) AS message_id,
 SAFE_CAST(count(*) AS INT) as unsubscribes
FROM {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}
