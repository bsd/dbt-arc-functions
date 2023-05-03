{% macro create_stg_stitch_sfmc_email_actions_rollup() %}

{% set relations = dbt_arc_functions.relations_that_match_regex(
    "^click$",
    is_source=True,
    source_name="stitch_sfmc_email",
    schema_to_search="src_stitch_sfmc_authorized",
) %}

select distinct cast(jobid as string) as message_id, safe_cast(null as int) as actions
from ({{ dbt_utils.union_relations(relations) }})
{% endmacro %}
