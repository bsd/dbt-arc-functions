{% macro create_stg_stitch_sfmc_email_opens_rollup(
    reference_name="stg_src_stitch_email_open"
) %}
    select
        safe_cast(job_id as string) as message_id,
        safe_cast(count(subscriber_key) as int) as opens
    from {{ ref(reference_name) }}
    group by 1
{% endmacro %}
