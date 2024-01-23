{% macro create_stg_audience_channel_by_day_cross_join(
    person_and_transaction="stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction"
) %}

{{
    config(
        materialized="table",
        partition_by={"field": "date_day", "data_type": "date", "granularity": "day"},
        cluster_by=["donor_audience"],
    )
}}

-- generate date spine 
with date_spine as (
        select date_day
        from
            unnest(
                generate_date_array(
                    (
                        select min(date_day),
                        from {{ ref(person_and_transaction) }}
                    ),
                    ifnull(
                        (
                            select max(date_day)
                            from {{ ref(person_and_transaction) }}
                        ),
                        current_date()
                    )
                )
            ) as date_day

    ),


-- Generate distinct combinations of audience and channel values
distinct_combinations as (
    select distinct donor_audience, channel
    from {{ ref(person_and_transaction) }}
)

-- Cross-join with distinct combinations and generated dates
select date_spine.date_day, distinct_combinations.donor_audience, distinct_combinations.channel
from distinct_combinations
cross join date_spine

{% endmacro %}






