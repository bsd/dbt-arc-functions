{% macro create_stg_stitch_sfmc_email_opens_rollup(
    reference_name='stg_src_stitch_email_open') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,
  SAFE_CAST(count(*) as INT) AS opens
FROM {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}
