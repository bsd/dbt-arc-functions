{% macro create_stg_stitch_sfmc_deliverability_bounces_daily_rollup(
    reference_name="stg_src_stitch_email_bounce"
) %}
    select
        safe_cast(event_dt as date) as sent_date,
        safe_cast(job_id as string) as message_id,
        safe_cast(domain as string) as email_domain,
        safe_cast(
            count(
                case
                    when lower(bounce_category) = 'hard bounce' and is_unique = true
                    then subscriber_key
                    else null
                end
            ) as int
        ) as hard_bounces,
        safe_cast(
            count(
                case
                    when lower(bounce_category) = 'soft bounce' and is_unique = true
                    then subscriber_key
                    else null
                end
            ) as int
        ) as soft_bounces
    from {{ ref(reference_name) }}
    group by 1, 2, 3
{% endmacro %}
