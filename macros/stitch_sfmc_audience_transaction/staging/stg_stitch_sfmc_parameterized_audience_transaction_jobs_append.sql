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
                "data_type": "month",
                "granularity": "month",
            },
        )
    }}


with

transactions as (
    select
    person_id,
    transaction_date_day,
    amount,
    recurring,
    transaction_id
    from {{ref(reference_name)}}

),

transactions_10_year as (
    select
    person_id,
    transaction_date_day,
    amount,
    recurring,
    transaction_id
    from {{ref(reference_name)}}
    where transaction_date_day >= date_sub(current_date(), interval 10 year)
),

join_dates as (
    select 
        person_id,
        date_created,
        first_transaction_date
    from {{ref(arc_person)}}
    ),


distinct_persons as (
    select distinct person_id from transactions_10_year),

date_spine as (
    select date_day
    from
        unnest(
            generate_date_array(
                (
                    select min(transaction_date_day),
                    from transactions_10_year 
                ),
                ifnull(
                    (
                        select max(transaction_date_day)
                        from transactions_10_year
                    ),
                    current_date()
                )
            )
        ) as date_day

    ),

cross_join as (
  select
    date_day, 
    person_id
  from date_spine
  cross join distinct_persons
),

calculations as (
    select
        transaction_date_day, 
        person_id,
        sum(amount) as total_amount,
        sum(case 
            when recurring = true 
            then amount else 0 end) 
        as recur_amount,
        count(distinct transaction_id) as num_transactions
    from transactions
    group by 1, 2
),

zero_transaction_days as (
    select 
        date_day as transaction_date_day,
        person_id,
        0 as total_amount,
        0 as recur_amount,
        0 as num_transactions
    from cross_join 
),

union_all as (
    select * from calculations
    UNION ALL
    select * from zero_transaction_days
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
        sum(c.num_transactions) over (
            partition by c.person_id order by c.transaction_date_day
        ) as cumulative_num_transactions_all_time,
        row_number() over (
            partition by c.person_id order by c.transaction_date_day
        ) as cumulative_num_transaction_days_all_time,
        jd.first_transaction_date,
        jd.date_created
    from union_all c
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
        /* parameterized field? */
        cast({{client_donor_audience}} as string) as donor_audience
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
