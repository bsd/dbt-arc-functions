{% macro create_stg_stitch_sfmc_email_actions_rollup(
    reference_name='stg_src_stitch_email_action') %}
Select SAFE_CAST(job_id AS string) as message_id,
SAFE_CAST(count(*) as INT) as actions
FROM {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}
