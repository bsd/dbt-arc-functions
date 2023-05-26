{% macro create_stg_stitch_sfmc_email_opens_rollup(
    reference_name="stg_src_stitch_email_open"
) %}

select safe_cast(jobid as string) as message_id, safe_cast(count(*) as int) as opens
from ({{ dbt_utils.union_relations(relations) }})
group by 1
{% endmacro %}
