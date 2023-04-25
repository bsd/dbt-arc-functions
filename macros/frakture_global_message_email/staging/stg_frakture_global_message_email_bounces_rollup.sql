{% macro create_stg_frakture_global_message_email_bounces_rollup(
    reference_name="stg_frakture_global_message_email_summary"
) %}
    select
        safe_cast(message_id as string) as message_id,
        sum(safe_cast(hard_bounces + soft_bounces as int)) as total_bounces,
        sum(safe_cast(0 as int)) as block_bounces,
        sum(safe_cast(0 as int)) as tech_bounces,
        sum(safe_cast(soft_bounces as int)) as soft_bounces,
        sum(safe_cast(hard_bounces as int)) as hard_bounces
    from {{ ref(reference_name) }}
    group by 1
{% endmacro %}
