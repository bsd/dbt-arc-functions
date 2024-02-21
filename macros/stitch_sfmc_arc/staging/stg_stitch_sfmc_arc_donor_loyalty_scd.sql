{% macro create_stg_stitch_sfmc_arc_donor_loyalty_scd(
    donor_transaction_enriched="stg_stitch_sfmc_parameterized_audience_transactions_enriched"
) %}

    with
        audience_union_transaction_joined as (

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
                transaction_enriched.channel as channel,
                transaction_enriched.appeal_business_unit,
                transaction_enriched.gift_size_string,
                transaction_enriched.recurring,
                transaction_enriched.amount,
                transaction_enriched.gift_count
            from {{ ref(donor_transaction_enriched) }} transaction_enriched

        ),
        donor_loyalty_counts as (

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
                    date(concat(fiscal_year, '-', '{{ var(' fiscal_year_start ') }}')),
                    interval 1 day
                ) as end_date,

                row_number() over (
                    partition by person_id order by fiscal_year desc
                ) as row_num
            from audience_union_transaction_joined
            group by person_id, fiscal_year
            order by person_id, fiscal_year
        ),
        donation_history as (
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

{% endmacro %}
