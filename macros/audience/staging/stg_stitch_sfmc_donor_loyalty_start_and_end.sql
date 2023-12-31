-- fmt: off
{% macro create_stg_stitch_sfmc_donor_loyalty_start_and_end(
    transaction_enriched="stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned"
) %}

/*

## Purpose
The purpose of this macro is to calculate donor loyalty-related metrics by determining the start and end dates for each fiscal year, understanding donation history, and classifying donors into different loyalty categories such as 'new donor', 'retained donor', 'retained 3+ donor', and 'reactivated donor'.

## Parameters
- `transaction_enriched`: Specifies the table containing enriched audience transactions data. Defaults to `stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned`.

## Configuration
The macro has a configured materialized table with partitioning and clustering settings. It's partitioned by the 'start_date' field with a daily granularity and clustered by 'donor_loyalty' for optimized storage and querying performance.

## Steps
1. **donor_loyalty_counts**: Determines start and end dates for each fiscal year, assigns a row number to each donor within a fiscal year for further analysis.
2. **donation_history**: Computes donation history for each donor, including the previous fiscal year, fiscal year before previous, and the last donation date.
3. **arc_donor_loyalty**: Derives donor loyalty status based on donor loyalty counts and donation history, classifying donors into specific loyalty categories.

## Output
The final output includes a table with a generated surrogate key ('id'), person ID, fiscal year, start and end dates for each fiscal year, and donor loyalty classification ('new donor', 'retained donor', 'retained 3+ donor', 'reactivated donor').

*/

    with
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
                    date(concat(fiscal_year, '-', '{{ var('fiscal_year_start') }}')),
                    interval 1 day
                ) as end_date,

                row_number() over (
                    partition by person_id order by fiscal_year desc
                ) as row_num
            from {{ ref(transaction_enriched) }}
            group by person_id, fiscal_year
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
            from {{ ref(transaction_enriched) }}
            group by person_id, fiscal_year
        )

    /*
Based on the data from donor_loyalty_counts and donation_history,
arc_donor_loyalty determines the donor's loyalty status for each fiscal year. 
It classifies donors as new, retained, retained with three or more years, 
or reactivated donors.
*/
    select
        {{dbt_utils.generate_surrogate_key(['donor_loyalty_counts.person_id', 'donor_loyalty_counts.fiscal_year'])}} as id,
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



{% endmacro %}
