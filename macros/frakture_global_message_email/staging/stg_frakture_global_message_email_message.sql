{% macro create_stg_frakture_global_message_email_message() %}
select distinct *
from {{ source("frakture_global_message_email", "global_message") }}
where message_id is not null and channel = 'email'
{% endmacro %}
