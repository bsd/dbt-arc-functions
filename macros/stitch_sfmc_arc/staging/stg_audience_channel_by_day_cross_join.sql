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
with
    date_spine as (
        select date_day
        from
            unnest(
                generate_date_array(
                    (select min(date_day), from {{ ref(person_and_transaction) }}),
                    (select max(date_day) from {{ ref(person_and_transaction) }})
                )
            ) as date_day

    ),

    -- Generate distinct combinations of audience and channel values
    distinct_audiences as (
        select distinct donor_audience
        from {{ ref(person_and_transaction) }}
        where donor_audience is not null
    ),

    distinct_channels as (
        select distinct channel
        from {{ ref(person_and_transaction) }}
        where channel is not null
    )

-- Cross-join with distinct combinations and generated dates
select date_day, donor_audience, channel
from date_spine
cross join distinct_audiences
cross join distinct_channels

{% endmacro %}
