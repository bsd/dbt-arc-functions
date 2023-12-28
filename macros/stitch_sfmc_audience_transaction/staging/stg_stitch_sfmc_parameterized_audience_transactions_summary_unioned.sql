{% macro create_stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned(
    where_clause_1
) %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_stitch_.*_transactions$"
    ) %}
    {{
        config(
            materialized="table",
            partition_by={
                "field": "transaction_date_day",
                "data_type": "date",
                "granularity": "day",
            },
        )
    }}

    with
        base as ({{ dbt_utils.union_relations(relations) }}),
        dedupe as (

            select
                base.*,
                cast(
                    timestamp_trunc(base.transaction_date, day) as date
                ) as transaction_date_day,
                -- deduping by transaction_id since there are multiple
                row_number() over (
                    partition by transaction_id order by transaction_date asc
                ) as row_number
            from base
            where
                transaction_date is not null
                and person_id is not null
                and amount > 0
                -- and only the last 10 years of transactions because we won't go
                -- further for
                -- audience data
                and transaction_date >= date_sub(current_date(), interval 10 year)
        )

    select *
    from dedupe
    where row_number = 1 {{ where_clause_1 }}

{% endmacro %}