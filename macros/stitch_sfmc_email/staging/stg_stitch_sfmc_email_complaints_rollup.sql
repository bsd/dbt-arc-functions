{% macro create_stg_stitch_sfmc_email_complaints_rollup(
    reference_name="stg_src_stitch_email_complaint"
) %}
    select
        safe_cast(jobid as string) as message_id,
        safe_cast(count(*) as int) as complaints
    from ({{ dbt_utils.union_relations(relations) }})
    group by 1
{% endmacro %}
