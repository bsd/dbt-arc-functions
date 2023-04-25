{% macro create_stg_frakture_everyaction_email_opens_rollup(
    reference_name="stg_frakture_everyaction_email_summary_unioned"
) %}
select
    safe_cast(message_id as string) as message_id,
    safe_cast(email_summary.email_opened as int) as opens
from {{ ref(reference_name) }} email_summary
{% endmacro %}
