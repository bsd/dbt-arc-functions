{% macro create_stg_frakture_global_message_email_actions_rollup(
    reference_name="stg_frakture_global_message_email_summary"
) %}
    select
        safe_cast(message_id as string) as message_id,
        sum(safe_cast(actions as int)) as actions
    from {{ ref(reference_name) }}
    group by 1
{% endmacro %}
