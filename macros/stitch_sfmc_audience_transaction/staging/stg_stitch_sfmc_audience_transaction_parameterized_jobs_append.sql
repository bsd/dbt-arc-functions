{% macro create_stg_stitch_sfmc_audience_transaction_parameterized_jobs_append(
    reference_name="stg_stitch_sfmc_audience_transactions_join"
    client_donor_audience="case
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
                end"
) %}

    with
        base as

        (
            select distinct
                transaction_date_day,
                person_id,
                ({{ client_donor_audience }}) as donor_audience,
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
                end as blue_state_donor_audience,
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
                blue_state_donor_audience,
                donor_engagement,
                row_number() over (
                    partition by
                        transaction_date_day,
                        person_id,
                        donor_audience,
                        blue_state_donor_audience,
                        donor_engagement
                    order by transaction_date_day desc
                ) as row_number
            from base
        )

    select transaction_date_day, person_id, donor_audience, blue_state_donor_audience donor_engagement
    from dedupe
    where row_number = 1

{% endmacro %}
