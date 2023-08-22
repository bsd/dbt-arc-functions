{% macro create_stg_stitch_sfmc_deliverability_jobs(
    reference_name="stg_src_stitch_email_sent"
) %}
    select
        safe_cast(job_id as string) as message_id,
        safe_cast(event_dt as date) as sent_date,
        safe_cast(domain as string) as email_domain
    from {{ ref(reference_name) }}
{% endmacro %}
