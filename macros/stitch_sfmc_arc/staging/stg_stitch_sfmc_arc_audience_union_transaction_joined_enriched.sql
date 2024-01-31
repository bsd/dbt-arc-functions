-- fmt: off
{% macro create_stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched(
    donor_audience_unioned="stg_stitch_sfmc_arc_audience_unioned",
    donor_engagement_by_day="stg_stitch_sfmc_donor_engagement_by_date_day",
    donor_transaction_enriched="stg_stitch_sfmc_parameterized_audience_transactions_enriched",
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

with audience_union_transaction_joined as (

        /*
 audience_union_transaction_joined combines data from donor_transaction_enriched, donor_audience_unioned,
 and donor_engagement_by_day by performing several joins based on common columns 
 like transaction_date_day and person_id. It selects various attributes from these 
 sources and calculates the fiscal year. The purpose is to create a consolidated dataset 
 that includes transaction details, audience information, and engagement data.
*/
    select
        transaction_enriched.transaction_date_day,
        {{
            dbt_arc_functions.get_fiscal_year(
                "transaction_enriched.transaction_date_day",
                var("fiscal_year_start"),
            )
        }} as fiscal_year,
        transaction_enriched.person_id,
        transaction_enriched.transaction_id,
        coalesce(audience_unioned.donor_audience, calculated_audience.donor_audience) as coalesced_audience,
        donor_engagement.donor_engagement,
        transaction_enriched.channel as channel,
        transaction_enriched.appeal_business_unit,
        transaction_enriched.gift_size_string,
        transaction_enriched.recurring,
        transaction_enriched.amount,
        transaction_enriched.gift_count,
    from {{ ref(donor_transaction_enriched) }} transaction_enriched
    left join
        {{ ref(donor_audience_unioned) }} audience_unioned
        on transaction_enriched.transaction_date_day = audience_unioned.date_day
        and transaction_enriched.person_id = audience_unioned.person_id
    left join 
        {{ref(jobs_append)}} calculated_audience 
        on transaction_enriched.transaction_date_day = calculated_audience.transaction_date_day
        and transaction_enriched.person_id = calculated_audience.person_id
    left join
        {{ ref(donor_engagement_by_day) }} donor_engagement
        on transaction_enriched.transaction_date_day = donor_engagement.date_day
        and transaction_enriched.person_id = donor_engagement.person_id

)

, donor_loyalty_counts as (

        /*

donor_loyalty_counts calculates donor loyalty-related information. 
It determines the start and end dates for each fiscal year, 
assigns a row number to each donor within a fiscal year, 
and organizes the data for further analysis.

*/
     select
        person_id,
        fiscal_year,
        min(transaction_date_day) as start_date,
        date_sub(
            date(concat(fiscal_year, '-', '{{ var('fiscal_year_start') }}')),
            interval 1 day
        ) as end_date,

        row_number() over (partition by person_id order by fiscal_year desc) as row_num
    from audience_union_transaction_joined
    group by person_id, fiscal_year
    order by person_id, fiscal_year        
    )

    ,    donation_history as (
        /*
donation_history computes the donation history for each donor, 
including the previous fiscal year, fiscal year before previous, 
and the last donation date.
*/
            select
                person_id,
                fiscal_year,
                lag(fiscal_year) over (
                    partition by person_id order by fiscal_year
                ) as previous_fiscal_year,
                lag(fiscal_year, 2) over (
                    partition by person_id order by fiscal_year
                ) as fiscal_year_before_previous,
                max(transaction_date_day) as last_donation_date
            from audience_union_transaction_joined
            group by person_id, fiscal_year
        )

        , arc_donor_loyalty as (
        /*
Based on the data from donor_loyalty_counts and donation_history,
arc_donor_loyalty determines the donor's loyalty status for each fiscal year. 
It classifies donors as new, retained, retained with three or more years, 
or reactivated donors.
*/
    select
        donor_loyalty_counts.person_id,
        donor_loyalty_counts.fiscal_year,
        donor_loyalty_counts.start_date,
        donor_loyalty_counts.end_date,
        case
            when donation_history.previous_fiscal_year is null
            then 'new_donor'
            when
                donation_history.previous_fiscal_year
                = donor_loyalty_counts.fiscal_year - 1
                and donation_history.fiscal_year_before_previous is null
            then 'retained_donor'
            when
                donation_history.previous_fiscal_year
                = donor_loyalty_counts.fiscal_year - 1
                and donation_history.fiscal_year_before_previous is not null
            then 'retained_3+_donor'
            when
                donation_history.previous_fiscal_year
                <> donor_loyalty_counts.fiscal_year - 1
            then 'reactivated_donor'
        end as donor_loyalty
    from donor_loyalty_counts
    left join
        donation_history
        on donor_loyalty_counts.person_id = donation_history.person_id
        and donor_loyalty_counts.fiscal_year = donation_history.fiscal_year
    order by donor_loyalty_counts.person_id, donor_loyalty_counts.fiscal_year

),

dedupe as (

        /*
the code selects data from audience_union_transaction_joined, 
left joins it with audience_calculated_alldates and arc_donor_loyalty, 
and creates a consolidated dataset. 
This dataset includes various attributes related to donors, 
such as transaction details, audience information, engagement data, 
loyalty status, and more.
making sure to finally dedupe on transaction_id.
*/
    select
        audience_union_transaction_joined.transaction_date_day,
        audience_union_transaction_joined.transaction_id,
        audience_union_transaction_joined.fiscal_year,
        audience_union_transaction_joined.person_id,
        audience_union_transaction_joined.coalesced_audience,
        audience_union_transaction_joined.donor_engagement,
        arc_donor_loyalty.donor_loyalty,
        audience_union_transaction_joined.channel,
        audience_union_transaction_joined.appeal_business_unit,
        audience_union_transaction_joined.gift_size_string,
        audience_union_transaction_joined.recurring,
        audience_union_transaction_joined.amount,
        1 as gift_count,
        row_number() over (partition by audience_union_transaction_joined.transaction_id order by audience_union_transaction_joined.transaction_date_day asc) as row_number
    from
        audience_union_transaction_joined
    left join
         arc_donor_loyalty
        on audience_union_transaction_joined.person_id = arc_donor_loyalty.person_id
        and audience_union_transaction_joined.transaction_date_day
        between arc_donor_loyalty.start_date and arc_donor_loyalty.end_date
    qualify row_number = 1

)

select 
    *, 
    row_number() over (
    partition by person_id, fiscal_year order by transaction_date_day
    ) as nth_transaction_this_fiscal_year 
from dedupe 

{% endmacro %}
