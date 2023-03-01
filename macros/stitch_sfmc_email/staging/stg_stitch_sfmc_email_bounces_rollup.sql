{% macro create_stg_stitch_sfmc_email_bounces_rollup(
    reference_name='stg_src_stitch_email_bounce') %}
SELECT
SAFE_CAST(job_id AS string) as message_id,
SUM(CASE WHEN bounce_category_id = '1' THEN 1 ELSE 0 END) AS hard_bounce,
SUM(CASE WHEN bounce_category_id = '2' THEN 1 ELSE 0 END) AS soft_bounce,
SUM(CASE WHEN bounce_category_id = '3' THEN 1 ELSE 0 END) AS block_bounce,
SUM(CASE WHEN bounce_category_id = '5' THEN 1 ELSE 0 END) AS tech_bounce,
SUM(CASE WHEN bounce_category_id = '4' THEN 1 ELSE 0 END) AS unknown_bounce,
SUM(CASE WHEN bounce_category_id = '1' THEN 1
WHEN bounce_category_id = '2' THEN 1 END) as total_bounces
FROM {{ ref(reference_name) }}
GROUP BY 1
{% endmacro %}
