{% macro create_stg_stitch_sfmc_email_recipients_rollup(
    reference_name="stg_src_stitch_email_sent"
) %}
    select
        safe_cast(job_id as string) as message_id,
        safe_cast(count(distinct subscriber_id) as int) as recipients
    from ({{ dbt_utils.union_relations(relations) }})
    group by 1
{% endmacro %}
