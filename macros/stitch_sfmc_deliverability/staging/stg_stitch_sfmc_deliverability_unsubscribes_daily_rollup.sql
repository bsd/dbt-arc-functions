{% macro create_stg_stitch_sfmc_deliverability_unsubscribes_daily_rollup(
    reference_name="stg_src_stitch_email_unsubscribe"
) %}
    select
        safe_cast(event_dt as date) as sent_date,
        safe_cast(job_id as string) as message_id,
        safe_cast(domain as string) as email_domain,
        count(
            case when is_unique = true then subscriber_key else null end
        ) as unsubscribes
    from {{ ref(reference_name) }}
    group by 1, 2, 3
{% endmacro %}