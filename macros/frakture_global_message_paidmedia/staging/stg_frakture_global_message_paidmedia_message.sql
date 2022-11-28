{% macro create_stg_frakture_global_message_paidmedia_message() %}
SELECT DISTINCT * FROM {{ source('frakture_global_message_paidmedia','global_message') }}
 WHERE message_id IS NOT NULL
 AND channel != 'email'
{% endmacro %}