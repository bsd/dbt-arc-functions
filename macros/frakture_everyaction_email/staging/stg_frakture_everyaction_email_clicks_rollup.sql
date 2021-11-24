{% macro create_stg_frakture_everyaction_email_clicks_rollup(
    reference_name='stg_frakture_everyaction_email_summary_unioned') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,
  SAFE_CAST(email_summary.email_clicked AS INT) AS clicks
FROM  {{ ref(reference_name) }} email_summary
{% endmacro %}