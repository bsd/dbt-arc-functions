{% macro create_stg_stitch_sfmc_deliverability_jobs_distinct() %}
    with
        unions as (
            select job_id, event_dt, domain
            from {{ ref("stg_src_stitch_email_action") }}
            union distinct
            select job_id, event_dt, domain
            from {{ ref("stg_src_stitch_email_bounce") }}
            union distinct
            select job_id, event_dt, domain
            from {{ ref("stg_src_stitch_email_click") }}
            union distinct
            select job_id, event_dt, domain
            from {{ ref("stg_src_stitch_email_open") }}
            union distinct
            select job_id, event_dt, domain
            from {{ ref("stg_src_stitch_email_unsubscribe") }}
        )

    select distinct
        safe_cast(job_id as string) as message_id,
        safe_cast(event_dt as date) as sent_date,
        safe_cast(domain as string) as email_domain
    from unions
{% endmacro %}
