{% macro create_stg_frakture_global_message_email_unioned() %}
SELECT DISTINCT * FROM {{ source('frakture_global_message_email','global_message') }}
 WHERE message_id IS NOT NULL
 AND channel = 'email'
{% endmacro %}