{% macro create_stg_stitch_sfmc_email_jobs(reference_name="stg_src_stitch_email_job") %}
select distinct
    safe_cast(job_id as string) as message_id,
    safe_cast(email_id as string) as email_id,
    safe_cast(from_name as string) as from_name,
    safe_cast(from_email as string) as from_email,
    safe_cast(
        coalesce(sched_dt, pickup_dt, delivered_dt) as timestamp
    ) as best_guess_timestamp,
    safe_cast(sched_dt as timestamp) as scheduled_timestamp,
    safe_cast(pickup_dt as timestamp) as pickup_timestamp,
    safe_cast(delivered_dt as timestamp) as delivered_timestamp,
    safe_cast(email_name as string) as email_name,
    safe_cast(email_subject as string) as email_subject,
    safe_cast(category as string) as category,
    safe_cast(null as string) as source_code
from {{ ref(reference_name) }}
{% endmacro %}
