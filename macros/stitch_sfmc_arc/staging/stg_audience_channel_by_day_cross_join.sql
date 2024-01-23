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

-- Calculate minimum and maximum dates from base table
with min_max_dates as (
    select min(date_day) as min_date, max(date_day) as max_date
    from {{ ref(person_and_transaction) }}
),

-- Generate all dates between min and max (inclusive)
date_array as (
    select generate_date_array(min_date, max_date) as date_day
    from min_max_dates
),

-- Generate distinct combinations of audience and channel values
distinct_combinations as (
    select distinct donor_audience, channel
    from {{ ref(person_and_transaction) }}
)

-- Cross-join with distinct combinations and generated dates
select date_array_unnest.date_day, distinct_combinations.donor_audience, distinct_combinations.channel
from distinct_combinations
cross join unnest(date_array) date_array_unnest

{% endmacro %}






