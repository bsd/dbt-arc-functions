{% macro create_stg_frakture_everyaction_email_recipients_rollup(
    reference_name='stg_frakture_everyaction_email_summary_unioned') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,
  SAFE_CAST(email_summary.email_sent AS INT) AS recipients
FROM  {{ ref(reference_name) }} email_summary
{% endmacro %}