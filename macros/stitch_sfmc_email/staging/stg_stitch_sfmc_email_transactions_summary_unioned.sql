{% macro create_stg_stitch_sfmc_transactions_summary_unioned() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_stitch_.*_transaction$"
    ) %}
    with base as ({{ dbt_utils.union_relations(relations) }})

    select
        base.*,
        cast(
            timestamp_trunc(base.transaction_date, day) as date
        ) as transaction_date_day
    from base

{% endmacro %}
