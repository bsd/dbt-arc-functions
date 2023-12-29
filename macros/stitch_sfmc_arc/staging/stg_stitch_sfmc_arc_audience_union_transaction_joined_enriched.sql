-- fmt: off
{% macro create_stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched(
    donor_audience_unioned="stg_stitch_sfmc_arc_audience_unioned",
    donor_engagement_by_day="stg_stitch_sfmc_donor_engagement_by_date_day",
    donor_loyalty='stg_stitch_sfmc_donor_loyalty_start_and_end',
    transaction_enriched="stg_stitch_sfmc_parameterized_audience_transactions_enriched",
    jobs_append="stg_stitch_sfmc_parameterized_audience_transaction_jobs_append"
) %}

{{ config(
    materialized='table',
    partition_by={
      "field": "transaction_date_day",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["recurring"]
)}}




/*
the code selects data from audience_union_transaction_joined, 
left joins it with audience_calculated_alldates and arc_donor_loyalty, 
and creates a consolidated dataset. 
This dataset includes various attributes related to donors, 
such as transaction details, audience information, engagement data, 
loyalty status, and more.
making sure to finally dedupe on transaction_id.
*/
with base as (

    select
        transaction_enriched.transaction_date_day,
        transaction_enriched.fiscal_year,
        transaction_enriched.person_id,
        transaction_enriched.transaction_id,
        audience_unioned.donor_audience as audience_unioned,
        calculated_audience.donor_audience as audience_calculated,
        coalesce(audience_unioned.donor_audience, calculated_audience.donor_audience) as coalesced_audience,
        donor_engagement.donor_engagement,
        donor_loyalty.donor_loyalty,
        transaction_enriched.best_guess_inbound_channel as channel,
        transaction_enriched.appeal_business_unit,
        transaction_enriched.gift_size_string,
        transaction_enriched.recurring,
        transaction_enriched.amount,
        transaction_enriched.gift_count,
        case
            when audience_union_transaction_joined.donor_audience is not null
            then 'unioned_donor_audience'
            else 'calculated_donor_audience'
        end as source_column,
        row_number() over (partition by transaction_enriched.transaction_id order by transaction_enriched.transaction_date_day asc) as row_number
    from {{ ref(transaction_enriched) }} transaction_enriched
    left join {{ref(jobs_append)}} calculated_audience
    on transaction_enriched.transaction_date_day = calculated_audience.transaction_date_day
    and transaction_enriched.person_id = calculated_audience.person_id
    left join
        {{ ref(donor_audience_unioned) }} audience_unioned
        on transaction_enriched.transaction_date_day = audience_unioned.date_day
        and transaction_enriched.person_id = audience_unioned.person_id
    left join
        {{ ref(donor_engagement_by_day) }} donor_engagement
        on transaction_enriched.transaction_date_day = donor_engagement.date_day
        and transaction_enriched.person_id = donor_engagement.person_id
    left join
        {{ref(donor_loyalty)}} donor_loyalty
        on transaction_enriched.person_id = donor_loyalty.person_id
        and transaction_enriched.transaction_date_day 
        between donor_loyalty.start_date and donor_loyalty.end_date

)

select * from base
where row_number = 1

{% endmacro %}
