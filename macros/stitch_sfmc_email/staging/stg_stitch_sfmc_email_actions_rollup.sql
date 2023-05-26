{% macro create_stg_stitch_sfmc_email_actions_rollup(
    reference_name="stg_src_stitch_email_actions"
) %}

    select distinct safe_cast(jobid as string) as message_id, null as actions
    from ({{ dbt_utils.union_relations(relations) }})
{% endmacro %}
