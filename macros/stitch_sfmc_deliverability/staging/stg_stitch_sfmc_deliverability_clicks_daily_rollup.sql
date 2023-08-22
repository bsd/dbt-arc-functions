{% macro create_stg_stitch_sfmc_deliverability_clicks_daily_rollup(
    reference_name="stg_src_stitch_email_click"
) %}
    select
        safe_cast(event_dt as date) as sent_date,
        safe_cast(job_id as string) as message_id,
        safe_cast(domain as string) as email_domain,
        count(case when is_unique = true then 1 else 0 end) as clicks
    from {{ ref(reference_name) }}
    where event_dt is not null and job_id is not null
    group by 1, 2, 3
{% endmacro %}
