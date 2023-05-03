{% macro create_stg_stitch_sfmc_email_bounces_rollup() %}
{% set relations = dbt_arc_functions.relations_that_match_regex(
    "^bounce$",
    is_source=True,
    source_name="stitch_sfmc_email",
    schema_to_search="src_stitch_sfmc_authorized",
) %}

select
    cast(jobid as string) as message_id,
    SAFE_CAST(sum(case when bouncecategoryid = 1 then 1 else 0 end) as int) as hard_bounces,
    SAFE_CAST(sum(case when bouncecategoryid = 2 then 1 else 0 end) as int) as soft_bounces,
    SAFE_CAST(sum(case when bouncecategoryid = 3 then 1 else 0 end) as int) as block_bounces,
    SAFE_CAST(sum(case when bouncecategoryid = 5 then 1 else 0 end) as int) as tech_bounces,
    SAFE_CAST(sum(case when bouncecategoryid = 4 then 1 else 0 end) as int) as unknown_bounces,
    SAFE_CAST(sum(
        case
            when bouncecategoryid = 1 then 1 when bouncecategoryid = 2 then 1
        end
    ) as int) as total_bounces
from ({{ dbt_utils.union_relations(relations) }})
group by 1
{% endmacro %}
