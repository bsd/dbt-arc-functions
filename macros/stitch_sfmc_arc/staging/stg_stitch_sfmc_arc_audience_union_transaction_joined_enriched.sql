-- fmt: off
{% macro create_stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched(
    donor_audience_unioned="stg_stitch_sfmc_arc_audience_unioned",
    donor_engagement_by_day="stg_stitch_sfmc_donor_engagement_by_date_day",
    donor_transaction_enriched="stg_stitch_sfmc_parameterized_audience_transactions_enriched",
    donor_loyalty_scd = "stg_stitch_sfmc_arc_donor_loyalty_scd",
    jobs_append="stg_stitch_sfmc_parameterized_audience_transaction_jobs_append"
) %}

{{ config(
    materialized='incremental',
    unique_key='transaction_id'
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
        coalesce(audience_unioned.donor_audience, calculated_audience.donor_audience) as donor_audience,
        donor_engagement.donor_engagement,
        transaction_enriched.channel as channel,
        transaction_enriched.appeal_business_unit,
        transaction_enriched.gift_size_string,
        transaction_enriched.recurring,
        transaction_enriched.amount,
        transaction_enriched.gift_count
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
        audience_union_transaction_joined.donor_audience,
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
        {{ref(donor_loyalty_scd)}} arc_donor_loyalty
        on audience_union_transaction_joined.person_id = arc_donor_loyalty.person_id
        and audience_union_transaction_joined.transaction_date_day
        between arc_donor_loyalty.start_date and arc_donor_loyalty.end_date
    qualify row_number = 1

), 

final as (

select 
    *, 
    row_number() over (
    partition by person_id, fiscal_year order by transaction_date_day
    ) as nth_transaction_this_fiscal_year 
from dedupe 
)

select *,
case
    when nth_transaction_this_fiscal_year = 1 then true else false
    end as is_first_transaction_this_fy
 from final 
{% if is_incremental() and target.name == 'prod'%}
-- pulls in all records within 7 days of max transaction date day
where transaction_date_day >= (select date_sub(max(transaction_date_day), interval 7 day) from {{ this }})
{% else %}
where transaction_date_day >= date_sub(current_date(), interval 2 year)
{% endif %}


{% endmacro %}
