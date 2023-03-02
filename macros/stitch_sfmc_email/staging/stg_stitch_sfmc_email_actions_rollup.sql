{% macro create_stg_stitch_sfmc_email_actions_rollup(
    reference_name='stg_src_stitch_email_action') %}
Select  distinct SAFE_CAST(job_id AS string) as message_id,
null as actions
FROM {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}
