{% macro create_stg_stitch_sfmc_donor_engagement_scd(
    donor_engagement="stg_stitch_sfmc_audience_transaction_person_engagement_with_start_and_end_dates",
    donor_engagement_date_spine="stg_stitch_sfmc_donor_engagement_date_spine"
) %}

with
    changes as (
        select
            person_id,
            start_day as transaction_date_day,
            donor_engagement,
            lag(donor_engagement) over (
                partition by person_id order by date_day
            ) as prev_donor_engagement
        from {{ ref(donor_engagement) }}
    )
select
    person_id,
    min(transaction_date_day) as start_date,
    ifnull(
        max(next_date) - 1,
        (select max(date) from {{ ref(donor_engagement_date_spine) }})
    ) as end_date,
    donor_engagement
from
    (
        select
            person_id,
            transaction_date_day,
            donor_engagement,
            lead(transaction_date_day) over (
                partition by person_id order by transaction_date_day
            ) as next_date
        from changes
        where prev_donor_engagement is null or donor_engagement != prev_donor_engagement
    ) filtered_changes
group by person_id, donor_engagement, next_date
order by person_id, start_date

{% endmacro %}
