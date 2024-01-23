{% macro create_stg_frakture_everyaction_email_bounces_rollup(
    reference_name="stg_frakture_everyaction_email_summary_unioned"
) %}
select
    safe_cast(message_id as string) as message_id,
    safe_cast(
        email_summary.email_hard_bounces + email_summary.email_soft_bounces as int
    ) as total_bounces,
    safe_cast(0 as int) as block_bounces,
    safe_cast(0 as int) as tech_bounces,
    safe_cast(email_summary.email_soft_bounces as int) as soft_bounces,
    safe_cast(email_summary.email_hard_bounces as int) as hard_bounces
from {{ ref(reference_name) }} email_summary
{% endmacro %}
