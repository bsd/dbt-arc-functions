{% macro create_stg_stitch_sfmc_email_recipients_rollup(
    reference_name='stg_src_stitch_email_sent') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,
  COUNT(SAFE_CAST(subscriber_id AS INT)) AS recipients
FROM {{ ref(reference_name) }} 
GROUP BY 1
{% endmacro %}
