{% macro create_stg_stitch_sfmc_audience_transaction_jobs_append(
    reference_name="stg_stitch_sfmc_audience_transactions_join"
) %}

{% set bsd_client = var.database %}

{% if bsd_client == "bsd-arc-uusa" %}
select
    transaction_date,
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
from {{ ref(reference_name) }}

{% else %}

select
    transaction_date,
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
        then 'existing'
        when
            donated_current_fiscal_year_july_to_june = 1
            and donated_last_fiscal_year_july_to_june = 0
        then 'new_donor'
    end as donor_loyalty
from {{ ref(reference_name) }}

{% endif %}

{% endmacro %}
