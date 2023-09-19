{% macro create_stg_stitch_sfmc_donor_audience_calculated_scd(
    calculated_audience_with_date_spine="stg_stitch_sfmc_donor_audience_calculated_with_date_spine",
    calculated_date_spine="stg_stitch_sfmc_audience_transaction_calculated_date_spine"
) %}

    with
        changes as (
            select
                person_id,
                transaction_date_day,
                donor_audience,
                lag(donor_audience) over (
                    partition by person_id order by transaction_date_day
                ) as prev_donor_audience
            from {{ ref(calculated_audience_with_date_spine) }}
        )
    select
        person_id,
        min(transaction_date_day) as start_date,
        ifnull(
            max(next_date) - 1, (select max(date) from {{ ref(calculated_date_spine) }})
        ) as end_date,
        donor_audience
    from
        (
            select
                person_id,
                transaction_date_day,
                donor_audience,
                lead(transaction_date_day) over (
                    partition by person_id order by transaction_date_day
                ) as next_date
            from changes
            where prev_donor_audience is null or donor_audience != prev_donor_audience
        ) filtered_changes
    group by person_id, donor_audience, next_date
    order by person_id, start_date

{% endmacro %}
