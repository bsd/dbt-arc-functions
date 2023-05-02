{% macro create_stg_frakture_actionkit_email_unsubscribes_rollup(
    reference_name="stg_frakture_actionkit_email_summary_unioned"
) %}
    select
        safe_cast(message_id as string) as message_id,
        sum(safe_cast(email_unsubscribes as int)) as unsubscribes
    from {{ ref(reference_name) }}
    group by 1
{% endmacro %}
