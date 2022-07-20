{% macro create_stg_frakture_global_message_email_unsubscribes_rollup(
    reference_name='stg_frakture_global_message_email_summary_unioned') %}
SELECT SAFE_CAST(message_id AS STRING) AS message_id,
 SAFE_CAST(unsubscribes AS INT) AS unsubscribes
FROM {{ ref(reference_name) }} 
{% endmacro %}
