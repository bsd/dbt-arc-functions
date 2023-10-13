{% macro create_stg_stitch_sfmc_audience_transaction_jobs_append(
    reference_name="stg_stitch_sfmc_audience_transactions_summary_unioned"
) %}

    with
        calculations as (
            select
                transaction_date_day,
                person_id,
                sum(amount) as daily_revenue,
                -- Calculate cumulative amount for the past 12 months
                sum(amount) over (
                    partition by person_id
                    order by unix_seconds(timestamp(transaction_date_day))  -- Convert date to Unix timestamp
                    range between 31556952 preceding and current row  -- 31,556,952 seconds in 12 months
                ) as cumulative_amount_12_months,
                -- Calculate cumulative amount for the past 24 months
                sum(amount) over (
                    partition by person_id
                    order by unix_seconds(timestamp(transaction_date_day))  -- Convert date to Unix timestamp
                    range between 63113904 preceding and current row  -- 63,113,904 seconds in 24 months
                ) as cumulative_amount_24_months,
                case when recurring = false then sum(amount) over (
                    partition by person_id
                    order by unix_seconds(timestamp(transaction_date_day))  -- Convert date to Unix timestamp
                    range between 63113904 preceding and current row  -- 63,113,904 seconds in 24 months
                ) end as cumulative_amount_24_months_non_recur,
                -- Calculate cumulative amount for the past 30 days for recurring
                case
                    when recurring = true
                    then
                        sum(amount) over (
                            partition by person_id
                            order by unix_seconds(timestamp(transaction_date_day))  -- Convert date to Unix timestamp
                            range between 2592000 preceding and current row  -- 2,592,000 seconds in 30 days
                        )
                    else 0
                end as cumulative_amount_30_days_recur,
                -- add at least 1 recurring donation... EVER? (new definition of sustainer)
                sum(amount) over (
                    partition by person_id 
                    order by unix_seconds(timestamp(transaction_date_day))
                    range between 36816402 preceding and current row -- 36816402 seconds is 14 months
                ) as cumulative_amount_14_months
            from {{ ref(reference_name) }}
            group by transaction_date_day, person_id, amount, recurring
        )

        , day_person_rollup as (
            select 
            transaction_date_day,
            person_id,
            sum(daily_revenue) as daily_revenue,
            sum(cumulative_amount_12_months) as cumulative_amount_12_months,
            sum(cumulative_amount_24_months) as cumulative_amount_24_months,
            sum(cumulative_amount_24_months_non_recur) as cumulative_amount_24_months_non_recur,
            sum(cumulative_amount_30_days_recur) as cumulative_amount_30_days_recur, 
            sum(cumulative_amount_14_months) as cumulative_amount_14_months
            from calculations
            group by 1, 2
        )


        ,base as

        (
            select distinct
                transaction_date_day,
                person_id,
                case
                    when cumulative_amount_30_days_recur > 0 -- confirmed by laura to leave?
                    and daily_revenue < 80
                    then 'Monthly'
                    -- sustainers = at least 1 recur gift in BBCRM
                    when cumulative_amount_12_months >= 25000 -- check!
                    then 'Major'
                    when
                        cumulative_amount_24_months between 1000 and 24999 -- check! 
                        and daily_revenue >= 80
                        -- what if someone gave more than 24999 in 24 months but not in 12 months? won't fit into major OR recurring Or mass!!!
                    then 'Leadership Giving'
                    -- midlevel = cumulative $1,000 - $24,999 over 24 months (including all gifts)
                    when
                    cumulative_amount_24_months_non_recur < 1000
                    then 'Mass'
                    -- mass = non-recurring; $1-$999; not monthly, midlevel, major, or unite
                    else null
                end as bluestate_donor_audience,
                case
                    when cumulative_amount_12_months >= 25000
                    then 'Major'
                    when
                        cumulative_amount_24_months between 1000 and 24999
                        and cumulative_amount_12_months < 25000
                    then 'Leadership Giving'
                    when
                        cumulative_amount_30_days_recur > 0
                        and cumulative_amount_24_months < 1000
                        and cumulative_amount_12_months < 25000
                    then 'Monthly'
                    when cumulative_amount_24_months between 1 and 999
                    then 'Mass'
                    else null
                end as donor_audience,
                case
                    when cumulative_amount_14_months < 1
                    then 'Lapsed'
                    -- lapsed = have not donated within 14 months; members of major, monthly, leadership giving should be excluded (according to UUSA)
                    else 'Active'
                    -- this isn't really counting active as much as everyone outside of mass that doesnt "lapse"?
                end as donor_engagement
            from day_person_rollup
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
