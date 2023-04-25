{% macro create_stg_frakture_everyaction_email_complaints_rollup(
    reference_name="stg_frakture_everyaction_email_summary_unioned"
) %}
select
    safe_cast(message_id as string) as message_id,
    sum(safe_cast(email_spam_complaints as int)) as complaints
from {{ ref(reference_name) }}
group by 1
{% endmacro %}
