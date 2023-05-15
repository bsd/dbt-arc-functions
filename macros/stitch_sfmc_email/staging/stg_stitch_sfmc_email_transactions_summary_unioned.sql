{% macro create_stg_stitch_sfmc_transactions_summary_unioned() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_stitch_.*_transactions$"
    ) %}
    with base as ({{ dbt_utils.union_relations(relations) }})

    select
        *
        from base
    where lower(inbound_channel) = 'web' -- web is the only channel that would contain email

{% endmacro %}
