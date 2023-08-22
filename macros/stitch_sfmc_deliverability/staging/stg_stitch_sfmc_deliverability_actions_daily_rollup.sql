{% macro create_stg_stitch_sfmc_deliverability_actions_daily_rollup(
    reference_name="stg_src_stitch_email_click"
) %}
    select
        safe_cast(event_dt as date) as sent_date,
        safe_cast(job_id as string) as message_id,
        safe_cast(domain as string) as email_domain,
        0 as actions
    from {{ ref(reference_name) }}
    where event_dt is not null and job_id is not null
    group by 1, 2, 3
{% endmacro %}
