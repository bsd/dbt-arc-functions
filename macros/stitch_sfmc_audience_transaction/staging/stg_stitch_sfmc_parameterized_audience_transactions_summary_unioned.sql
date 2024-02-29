{% macro create_stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned(
    channel="NULL", digital_status="NULL"
) %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_stitch_.*_transactions$"
    ) %}
    {{
        config(
            materialized="table",
            partition_by={
                "field": "transaction_date_day",
                "data_type": "month",
                "granularity": "month",
            },
        )
    }}

    with
        base as ({{ dbt_utils.union_relations(relations) }}),
        dedupe as (

            select
                *,
                cast(
                    timestamp_trunc(transaction_date, day) as date
                ) as transaction_date_day,
                -- deduping by transaction_id since there are multiple
                row_number() over (
                    partition by transaction_id order by transaction_date asc
                ) as row_number
            from base
            where transaction_date is not null and person_id is not null and amount > 0
        )

    select
        _dbt_source_relation,
        transaction_id,
        transaction_date,
        transaction_date_day,
        person_id,
        recurring,
        initcap({{ channel }}) as channel,
        cast({{ digital_status }} as boolean) as is_digital,
        appeal_business_unit,
        appeal,
        amount
    from dedupe
    where row_number = 1

{% endmacro %}
