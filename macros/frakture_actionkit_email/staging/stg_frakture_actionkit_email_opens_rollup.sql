{% macro create_stg_frakture_actionkit_email_opens_rollup(
    reference_name="stg_frakture_actionkit_email_summary_unioned"
) %}
select
    safe_cast(message_id as string) as message_id,
    sum(safe_cast(email_opened as int)) as opens
from {{ ref(reference_name) }}
group by 1
{% endmacro %}
