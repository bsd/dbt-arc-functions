{% macro create_stg_stitch_sfmc_audience_transaction_jobs_append(
    reference_name="stg_stitch_sfmc_audience_transactions_join"
) %}

    with
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
                end as donor_engagement,
                -- june to july is their fiscal year
                case
                    when
                        donated_current_fiscal_year_july_to_june = 1
                        and donated_last_fiscal_year_july_to_june = 1
                        and donated_two_fiscal_years_ago_july_to_june = 0
                        and donated_three_fiscal_years_ago_july_to_june = 0
                    -- and did not donate two years ago and before
                    then 'retained'
                    -- reinstated missing
                    when
                        donated_current_fiscal_year_july_to_june = 1
                        and donated_last_fiscal_year_july_to_june = 0
                        and (
                            donated_two_fiscal_years_ago_july_to_june = 1
                            or donated_three_fiscal_years_ago_july_to_june = 1
                            or donated_four_fiscal_years_ago_july_to_june = 1
                        )
                    then 'reinstated'
                    when
                        donated_current_fiscal_year_july_to_june = 1
                        and donated_last_fiscal_year_july_to_june = 0
                    then 'new_donor'
                    when
                        donated_current_fiscal_year_july_to_june = 1
                        and donated_last_fiscal_year_july_to_june = 1
                        and donated_two_fiscal_years_ago_july_to_june = 1
                    -- and any other year before that
                    then 'retained 3+'
                -- retained 3+ also multiyear
                end as donor_loyalty
            from {{ ref(reference_name) }}
        ),
        dedupe as (
            select
                transaction_date_day,
                person_id,
                donor_audience,
                donor_engagement,
                donor_loyalty,
                row_number() over (
                    partition by
                        transaction_date_day,
                        person_id,
                        donor_audience,
                        donor_engagement,
                        donor_loyalty
                    order by transaction_date_day desc
                ) as row_number
            from base
        )

    select
        transaction_date_day, person_id, donor_audience, donor_engagement, donor_loyalty
    from dedupe
    where row_number = 1

{% endmacro %}
