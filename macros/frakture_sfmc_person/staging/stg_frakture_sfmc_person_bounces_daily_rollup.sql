{% macro create_stg_frakture_sfmc_person_bounces_daily_rollup(
    reference_name="stg_frakture_sfmc_person_message_stat_unioned_with_domain"
) %}
    select
        safe_cast(sent_ts as date) as sent_date,
        safe_cast(message_id as string) as message_id,
        safe_cast(email_domain as string) as email_domain,
        sum(safe_cast(bounced as int)) as hard_bounces,
        sum(safe_cast(0 as int)) as soft_bounces
    from {{ ref(reference_name) }}
    group by 1, 2, 3
{% endmacro %}
