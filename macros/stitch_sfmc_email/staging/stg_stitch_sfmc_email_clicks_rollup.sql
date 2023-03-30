{% macro create_stg_stitch_sfmc_email_clicks_rollup(
    reference_name='stg_src_stitch_email_click') %}
Select SAFE_CAST(job_id AS string) as message_id,
        SAFE_CAST(count(*) as INT) as clicks
FROM {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}
