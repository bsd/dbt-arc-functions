{% macro create_stg_frakture_global_message_email_summary() %}
SELECT DISTINCT * FROM {{ source('frakture_global_message_email','global_message_summary_by_date') }}
 WHERE message_id IS NOT NULL
 AND channel = 'email'
{% endmacro %}
