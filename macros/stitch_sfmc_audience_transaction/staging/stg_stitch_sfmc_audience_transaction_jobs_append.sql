{% macro create_stg_stitch_sfmc_audience_transaction_jobs_append(
    reference_name="stg_stitch_sfmc_audience_transactions_summary_unioned"
) %}

    with
        calculations as (
            select
                transaction_date_day,
                person_id,
                -- Calculate cumulative amount for the past 12 months
                sum(amount) over (
                    partition by person_id
                    order by unix_seconds(transaction_date_day)  -- Convert date to Unix timestamp
                    range between 31556952 preceding and current row  -- 31,556,952 seconds in 12 months
                ) as cumulative_amount_12_months,
                -- Calculate cumulative amount for the past 24 months
                sum(amount) over (
                    partition by person_id
                    order by unix_seconds(transaction_date_day)  -- Convert date to Unix timestamp
                    range between 63113904 preceding and current row  -- 63,113,904 seconds in 24 months
                ) as cumulative_amount_24_months,
                -- Calculate cumulative amount for the past 30 days for recurring
                -- transactions
                case
                    when recurring = true
                    then
                        sum(amount) over (
                            partition by person_id
                            order by unix_seconds(transaction_date_day)  -- Convert date to Unix timestamp
                            range between 2592000 preceding and current row  -- 2,592,000 seconds in 30 days
                        )
                    else 0
                end as cumulative_amount_30_days_recur,
                max(
                    case
                        when
                            transaction_date >= date_add(
                                date_trunc(transaction_date_day, month),
                                interval - 14 month
                            )
                        then 1
                        else 0
                    end
                ) as donated_within_14_months
            from {{ ref(reference_name) }}
        ),
        base as

        (
            select distinct
                transaction_date_day,
                person_id,
                case
                    when cumulative_amount_12_months >= 25000
                    then 'major'
                    when
                        cumulative_amount_24_months between 1000 and 24999
                        and cumulative_amount_12_months < 25000
                    then 'midlevel'
                    when
                        cumulative_amount_30_days_recur > 0
                        and cumulative_amount_24_months < 1000
                        and cumulative_amount_12_months < 25000
                    then 'recurring'
                    when cumulative_amount_24_months between 1 and 999
                    then 'grassroots'
                    else null
                end as donor_audience,
                case
                    when donated_within_14_months = 0
                    then 'lapsed'
                    when donated_within_14_months = 1
                    then 'active'
                    else null
                end as donor_engagement
            from {{ ref(reference_name) }}
        ),
        dedupe as (
            select
                transaction_date_day,
                person_id,
                donor_audience,
                donor_engagement,
                row_number() over (
                    partition by
                        transaction_date_day,
                        person_id,
                        donor_audience,
                        donor_engagement
                    order by transaction_date_day desc
                ) as row_number
            from base
        )

    select transaction_date_day, person_id, donor_audience, donor_engagement
    from dedupe
    where row_number = 1

{% endmacro %}
