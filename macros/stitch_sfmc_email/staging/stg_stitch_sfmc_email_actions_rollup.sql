{% macro create_stg_stitch_sfmc_email_actions_rollup(
    reference_name="stg_src_stitch_email_action"
) %}
select distinct safe_cast(job_id as string) as message_id, null as actions
from {{ ref(reference_name) }}
group by 1
{% endmacro %}
