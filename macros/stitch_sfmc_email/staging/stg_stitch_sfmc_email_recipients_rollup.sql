{% macro create_stg_stitch_sfmc_email_recipients_rollup(
    reference_name="stg_src_stitch_email_sent"
) %}
    select
        safe_cast(message_id as string) as message_id,
        count(safe_cast(subscriber_id as int)) as recipients
    from {{ ref(reference_name) }}
    group by 1
{% endmacro %}
