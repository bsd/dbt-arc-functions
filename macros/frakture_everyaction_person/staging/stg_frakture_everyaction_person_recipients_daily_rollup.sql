{% macro create_stg_frakture_everyaction_person_recipients_daily_rollup(
    reference_name="stg_frakture_everyaction_person_message_stat_unioned_with_domain"
) %}
    select
        safe_cast(sent_ts as date) as sent_date,
        safe_cast(message_id as string) as message_id,
        safe_cast(email_domain as string) as email_domain,
        sum(safe_cast(received as int)) as recipients
    from {{ ref(reference_name) }}
    group by 1, 2, 3
{% endmacro %}
