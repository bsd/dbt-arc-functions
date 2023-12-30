{% macro create_stg_stitch_sfmc_parameterized_calculated_audience(
    reference_name="stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned",
    client_donor_audience="NULL"
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
        calculations as (
            select
                transaction_date_day,
                person_id,
                sum(amount) as total_amount,
                sum(case when recurring = true then amount else 0 end) as recur_amount
            from {{ ref(reference_name) }}
            group by 1, 2
        ),
        day_person_rollup as (
            select
                transaction_date_day,
                person_id,
                -- Calculate cumulative recur and 1x amount for the past 24 months
                sum(total_amount) over (
                    partition by person_id
                    order by unix_seconds(timestamp(transaction_date_day))  -- Convert date to Unix timestamp
                    range between 63113904 preceding and current row  -- 63,113,904 seconds in 24 months
                ) as cumulative_amount_24_months,
                -- Calculate cumulative recurring amount over past 90 days
                sum(recur_amount) over (
                    partition by person_id
                    order by unix_seconds(timestamp(transaction_date_day))  -- Convert date to Unix timestamp
                    range between 7776000 preceding and current row  -- unix seconds in 90 days
                ) as cumulative_amount_90_days_recur
            from calculations
            group by transaction_date_day, person_id, total_amount, recur_amount
        ),
        base as

        (
            select distinct
                transaction_date_day,
                person_id,
                case
                    when cumulative_amount_24_months >= 25000
                    then 'Major'
                    when cumulative_amount_24_months between 1000 and 24999.99
                    then 'Leadership Giving'
                    when cumulative_amount_90_days_recur > 0
                    then 'Monthly'
                    else 'Mass'
                end as bluestate_donor_audience,  -- modeled after UUSA
                {{ client_donor_audience }} as donor_audience
            from day_person_rollup
        ),

        audience_calculated_dedupe as (
            /*
audience_calculated_dedupe retrieves calculated audience data for all dates 
*/
            select
                transaction_date_day,
                person_id,
                donor_audience,
                row_number() over (
                    partition by person_id, transaction_date_day
                    order by transaction_date_day
                ) as row_number
            from base

        ),

calculated_audience as (
    /*
 selects just one donor audience value for each person per day
*/
    select transaction_date_day, person_id, donor_audience
    from audience_calculated_dedupe
    where row_number = 1
)



        calc_date_spine as (
            select date
            from
                unnest(
                    generate_date_array(
                        (
                            select min(transaction_date_day),
                            from calculated_audience
                        ),
                        ifnull(
                            (
                                select
                                    min(
                                        date(
                                            cast(
                                                concat(
                                                    substr(dbt_valid_from, 0, 22),
                                                    " America/New_York"
                                                ) as timestamp
                                            ),
                                            "America/New_York"
                                        )
                                        - 1
                                    )

                                from {{ ref(audience_snapshot) }}
                            ),
                            current_date()
                        )
                    )
                ) as date

        ),

        calculated_with_date_spine as (

            select transaction_date_day, person_id, donor_audience
            from {{ ref(calculated_audience) }} calc_audience
            join calc_date_spine on calc_date_spine.date = calc_audience.transaction_date_day
            where
                calc_audience.transaction_date_day < (select max(date) from calc_date_spine)
                and donor_audience is not null

        ),
        calc_change as (
            select
                person_id,
                transaction_date_day,
                donor_audience,
                lag(donor_audience) over (
                    partition by person_id order by transaction_date_day
                ) as prev_donor_audience
            from calculated_with_date_spine
        ),
        calc_filtered_changes as (
            select
                person_id,
                transaction_date_day,
                donor_audience,
                lead(transaction_date_day) over (
                    partition by person_id order by transaction_date_day
                ) as next_date
            from calc_change
            where prev_donor_audience is null or donor_audience != prev_donor_audience

        ),

calculated_audience_scd as (
    select
        person_id,
        min(transaction_date_day) as start_date,
        ifnull(max(next_date) - 1, (select max(date) from date_spine)) as end_date,
        donor_audience
    from calc_filtered_changes
    group by person_id, donor_audience, next_date
    order by person_id, start_date
),

        calc_audience_by_date_day as (
            select
                date_spine.date as date_day,
                calculated_audience_scd.person_id,
                calculated_audience_scd.donor_audience
            from date_spine
            inner join
                calculated_audience_scd
                on date_spine.date >= date(calculated_audience_scd.start_date)
                and (
                    date_spine.date <= date(calculated_audience_scd.end_date)
                    or date(calculated_audience_scd.start_date) is null
                )
        ),
        dedup_calc_audience_by_date_day as (
            select
                date_day,
                person_id,
                donor_audience,
                row_number() over (
                    partition by date_day, person_id order by donor_audience
                ) as row_num
            from calc_audience_by_date_day

        )

    select date_day, person_id, donor_audience
    from dedup_calc_audience_by_date_day
    where row_num = 1


{% endmacro %}
