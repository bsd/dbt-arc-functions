{% macro create_stg_frakture_actionkit_email_bounces_rollup(
    reference_name="stg_frakture_actionkit_email_summary_unioned"
) %}
    select
        safe_cast(message_id as string) as message_id,
        sum(safe_cast(email_hard_bounces + email_soft_bounces as int)) as total_bounces,
        sum(safe_cast(email_blocked as int)) as block_bounces,
        sum(safe_cast(0 as int)) as tech_bounces,
        sum(safe_cast(email_soft_bounces as int)) as soft_bounces,
        sum(safe_cast(email_hard_bounces as int)) as hard_bounces
    from {{ ref(reference_name) }}
    group by 1
{% endmacro %}
