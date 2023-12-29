{% macro create_stg_stitch_sfmc_audience_transaction_person_engagement_with_start_and_end_dates(
    stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned="stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned"
) %}

/*
    This macro generates a table that calculates the start and end dates for donor engagements (active and lapsed) based on transaction history.

    Parameters:
    - stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned: Name of the summary table containing transaction data.

    Returns:
    - Resulting table with person IDs, donor engagements, start dates, and inferred end dates.
    */

    with
        start_of_active_and_lapsed as (
            -- Identifies the start of active and lapsed donor engagements based on transaction dates.
            select
                person_id,
                transaction_date,
                -- Calculates the start of the active engagement.
                case
                -- If there's no previous transaction, consider the current transaction date as the start of active engagement
                    when
                        lag(transaction_date) over (
                            partition by person_id order by transaction_date
                        )
                        is null
                    then transaction_date
                    -- If the gap between the current and previous transaction exceeds 426 days, consider it a new active engagement
                    when
                        date_diff(
                            transaction_date,
                            lag(transaction_date) over (
                                partition by person_id order by transaction_date
                            ),
                            day
                        )
                        > 426
                    then transaction_date
                end as start_of_active,
                -- Calculates the start of the lapsed engagement.
                case
                -- If the gap between the current and next transaction exceeds 426 days, consider it the start of lapsed engagement
                    when
                        date_diff(
                            lead(transaction_date) over (
                                partition by person_id order by transaction_date
                            ),
                            transaction_date,
                            day
                        )
                        > 426
                    then date_add(transaction_date, interval 426 day)
                -- If there's no next transaction and the gap between the current transaction and today exceeds 426 days, mark it as lapsed
                    when
                        lead(transaction_date) over (
                            partition by person_id order by transaction_date
                        )
                        is null
                        and date_diff(current_date, transaction_date, day) > 426
                    then date_add(transaction_date, interval 426 day)
                end as start_of_lapsed
            from
                (
                    -- Selects distinct person IDs with their transaction dates from the specified summary table.
                    select distinct
                        person_id, date(transaction_date) as transaction_date
                    from
                        {{
                            ref(
                                "stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned"
                            )
                        }}
                ) as person_with_all_transaction_dates

            order by 1, 2
        ),
        donor_engagement_start_dates as (
            -- Collects the start dates for active and lapsed engagements.
            select
                person_id, 'active' as donor_engagement, start_of_active as start_date,
            from start_of_active_and_lapsed
            where start_of_active is not null
            union all
            select
                person_id, 'lapsed' as donor_engagement, start_of_lapsed as start_date,
            from start_of_active_and_lapsed
            where start_of_lapsed is not null
            order by 1, 3
        )
    select
        person_id,
        donor_engagement,
        start_date,
        -- Determines the end date as the day before the next start date (or today) for each person and engagement type
        lead(start_date) over (partition by person_id order by start_date)
        - 1 as end_date
    from donor_engagement_start_dates
    order by 1, 3

{% endmacro %}
