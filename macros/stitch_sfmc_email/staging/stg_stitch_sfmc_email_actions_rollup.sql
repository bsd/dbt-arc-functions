
{% macro create_stg_stitch_sfmc_email_actions_rollup(
    reference_name="stg_stitch_sfmc_email_actions"
) %}
select distinct message_id, sum(actions) as actions
from {{ ref(reference_name) }}
group by 1

select distinct safe_cast(jobid as string) as message_id, null as actions
from ({{ dbt_utils.union_relations(relations) }})
{% endmacro %}
