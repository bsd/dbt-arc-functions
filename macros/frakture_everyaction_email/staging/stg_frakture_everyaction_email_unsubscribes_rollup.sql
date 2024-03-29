{% macro create_stg_frakture_everyaction_email_unsubscribes_rollup(
    reference_name="stg_frakture_everyaction_email_summary_unioned"
) %}
    select
        safe_cast(message_id as string) as message_id,
        safe_cast(email_summary.email_unsubscribes as int) as unsubscribes
    from {{ ref(reference_name) }} email_summary
{% endmacro %}
