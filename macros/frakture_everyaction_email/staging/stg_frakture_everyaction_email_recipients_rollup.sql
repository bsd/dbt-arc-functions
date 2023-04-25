{% macro create_stg_frakture_everyaction_email_recipients_rollup(
    reference_name="stg_frakture_everyaction_email_summary_unioned"
) %}
    select
        safe_cast(message_id as string) as message_id,
        safe_cast(email_summary.email_sent as int) as recipients
    from {{ ref(reference_name) }} email_summary
{% endmacro %}
