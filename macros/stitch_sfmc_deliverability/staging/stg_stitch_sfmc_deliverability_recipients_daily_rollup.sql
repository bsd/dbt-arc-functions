{% macro create_stg_stitch_sfmc_deliverability_recipients_daily_rollup(
    reference_name="stg_src_stitch_email_sent"
) %}
    with
        unique_recipients as (
            select
                safe_cast(event_dt as date) as sent_date,
                safe_cast(job_id as string) as message_id,
                safe_cast(domain as string) as email_domain,
                subscriber_key,
                row_number() over (
                    partition by job_id, subscriber_key order by event_dt
                ) as recipient_row_number
            from {{ ref(reference_name) }}
        )

    select
        sent_date,
        message_id,
        email_domain,
        count(
            distinct subscriber_key ) as recipients
    from unique_recipients
    where recipient_row_number = 1
    group by 1, 2, 3
{% endmacro %}
