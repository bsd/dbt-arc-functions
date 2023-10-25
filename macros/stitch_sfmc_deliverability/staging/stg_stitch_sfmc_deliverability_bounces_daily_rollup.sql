{% macro create_stg_stitch_sfmc_deliverability_bounces_daily_rollup(
    reference_name="stg_src_stitch_email_bounce"
) %}

    with
        base as (
            select
                event_dt,
                job_id,
                domain,
                subscriber_key,
                case when bounce_category_id = '1' then 1 else 0 end as hard_bounces,
                case when bounce_category_id = '2' then 1 else 0 end as soft_bounces,
                case
                    when bounce_category_id is not null then 1 else 0
                end as total_bounces
            from {{ ref(reference_name) }}

        ),
        unique_bounces as (
            select
                safe_cast(event_dt as date) as sent_date,
                safe_cast(job_id as string) as message_id,
                safe_cast(domain as string) as email_domain,
                subscriber_key,
                hard_bounces,
                soft_bounces,
                total_bounces,
                row_number() over (
                    partition by job_id, subscriber_key order by event_dt
                ) as bounce_row_number,
            from base

        )

    select
        sent_date,
        message_id,
        email_domain,
        sum(hard_bounces) as hard_bounces,
        sum(soft_bounces) as soft_bounces,
        sum(total_bounces) as total_bounces
    from unique_bounces
    where bounce_row_number = 1
    group by 1, 2, 3
{% endmacro %}
