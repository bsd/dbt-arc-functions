{% macro create_stg_frakture_everyaction_person_jobs(
    reference_name="stg_frakture_everyaction_person_message_stat_unioned_with_domain"
) %}
    select
        safe_cast(message_id as string) as message_id,
        safe_cast(sent_ts as date) as sent_date,
        safe_cast(email_domain as string) as email_domain
    from {{ ref(reference_name) }}
{% endmacro %}
