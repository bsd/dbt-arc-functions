{% macro create_stg_stitch_sfmc_email_unsubscribes_rollup() %}
{% set relations = dbt_arc_functions.relations_that_match_regex(
    "^unsubscribe$",
    is_source=True,
    source_name="stitch_sfmc_email",
    schema_to_search="src_stitch_sfmc_authorized",
) %}
select cast(jobid as string) as message_id, safe_cast(count(*) as int) as unsubscribes
from ({{ dbt_utils.union_relations(relations) }})
group by 1
{% endmacro %}
