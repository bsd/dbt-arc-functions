{% macro create_stg_frakture_global_message_email_summary() %}
    select distinct *
    from {{ source("frakture_global_message_email", "global_message_summary_by_date") }}
    where message_id is not null and channel = 'email'
{% endmacro %}
