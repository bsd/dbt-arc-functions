{% macro create_stg_stitch_sfmc_email_ftaf_rollup(
    reference_name='stg_src_stitch_email_ftaf') %}
Select SAFE_CAST(job_id AS string) as message_id,
count(*) as ftaf
FROM {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}
