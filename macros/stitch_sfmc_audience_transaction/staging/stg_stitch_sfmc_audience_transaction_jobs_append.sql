{% macro create_stg_stitch_sfmc_audience_transaction_jobs_append(
    reference_name="stg_stitch_sfmc_audience_transactions_join"
) %}

{% set bsd_client = var.bsd_client %}

-- TODO: prompt user to enter client name if applicable during create or update set up?
-- this statement is meant to be unique for every date, transaction_id, and person_id 
-- with the intent of joining it back to the transaction table
{% if bsd_client == "bsd-arc-uusa" %}
-- This SQL statement will be used if 'variable' has a value
select
    transaction_date,
    person_id,
    case
        when cumulative_amount_12_months >= 25000
        then 'major'
        when
            cumulative_amount_24months between 1000 and 24999
            and cumulative_amount_12_months < 25000
        then 'midlevel'
        when
            cumulative_amount_30_days_recur > 0
            and cumulative_amount_24months < 1000
            and cumulative_amount_12_months < 25000
        then 'recurring'
        when cumulative_amount between 1 and 999
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
        when
            donated_current_fiscal_year_july_to_june = 1
            and donated_last_fiscal_year_july_to_june = 0
        then 'new_donor'
        when
            donated_current_fiscal_year_july_to_june = 1
            and donated_last_fiscal_year_july_to_june = 1
            and (
                donated_two_fiscal_years_ago_july_to_june = 1
                or donated_three_fiscal_years_ago_july_to_june = 1
            )
        -- and any other year before that
        then 'retained 3+'
    -- retained 3+ also multiyear
    end as donor_loyalty
from {{ reference_name }}

{% else %}
-- This SQL statement will be used if 'variable' is empty or does not exist, it's
-- dummy for now
select
    transaction_date,
    person_id,
    case
        when cumulative_amount_12_months >= 25000
        then 'major'
        when
            cumulative_amount_24months between 1000 and 24999
            and cumulative_amount_12_months < 25000
        then 'midlevel'
        when
            cumulative_amount_30_days_recur > 0
            and cumulative_amount_24months < 1000
            and cumulative_amount_12_months < 25000
        then 'recurring'
        when cumulative_amount between 1 and 999
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
        then 'existing'
        when
            donated_current_fiscal_year_july_to_june = 1
            and donated_last_fiscal_year_july_to_june = 0
        then 'new_donor'
    end as donor_loyalty
from {{ reference_name }}

{% endif %}

{% endmacro %}
