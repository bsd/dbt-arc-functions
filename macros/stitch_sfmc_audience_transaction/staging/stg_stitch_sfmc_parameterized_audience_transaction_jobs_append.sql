{% macro create_stg_stitch_sfmc_parameterized_audience_transaction_jobs_append(
    reference_name="stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned",
    arc_person="stg_stitch_sfmc_arc_person",
    first_gift="stg_stitch_sfmc_parameterized_audience_transaction_first_gift",
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
                sum(case when recurring = true then amount else 0 end) as recur_amount,
                count(amount) as num_transactions
            from {{ ref(reference_name) }}
            group by 1, 2
        ),
        join_dates as (
            select p.person_id, p.date_created, p.first_transaction_date
            from {{ ref(arc_person) }} p
        ),
        day_person_rollup as (
            select
                c.transaction_date_day,
                c.person_id,
                sum(c.total_amount) over (
                    partition by c.person_id
                    order by unix_seconds(timestamp(c.transaction_date_day))
                    range between 63113904 preceding and current row
                ) as cumulative_amount_24_months,
                sum(c.recur_amount) over (
                    partition by c.person_id
                    order by unix_seconds(timestamp(c.transaction_date_day))
                    range between 7776000 preceding and current row
                ) as cumulative_amount_90_days_recur,
                sum(c.total_amount) over (
                    partition by c.person_id order by c.transaction_date_day
                ) as cumlative_amount_all_time,
                sum(c.num_transactions) over (partition by c.person_id
                    order by c.transaction_date_day
                ) as cumulative_num_transactions_all_time,
                row_number()
                    over (
                        partition by c.person_id order by c.transaction_date_day
                    )
                    as cumulative_num_transaction_days_all_time
                jd.first_transaction_date,
                jd.date_created
            from calculations c
            left join join_dates jd on c.person_id = jd.person_id
            group by
                c.transaction_date_day,
                c.person_id,
                c.total_amount,
                c.recur_amount,
                c.num_transactions,
                jd.date_created,
                jd.first_transaction_date
        ),
        base as (
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
        dedupe as (
            select
                transaction_date_day,
                person_id,
                donor_audience,
                row_number() over (
                    partition by transaction_date_day, person_id, donor_audience
                    order by transaction_date_day desc
                ) as row_number
            from base
        )
    select transaction_date_day, person_id, donor_audience
    from dedupe
    where row_number = 1

{% endmacro %}
