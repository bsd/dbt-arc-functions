{% macro create_stg_stitch_sfmc_deliverability_jobs_distinct(
    reference_name="stg_src_stitch_email_sent"
) %}
    select distinct
        safe_cast(job_id as string) as message_id,
        safe_cast(event_dt as date) as sent_date,
        safe_cast(domain as string) as email_domain
    from {{ ref(reference_name) }}
    where event_dt is not null and job_id is not null
{% endmacro %}
