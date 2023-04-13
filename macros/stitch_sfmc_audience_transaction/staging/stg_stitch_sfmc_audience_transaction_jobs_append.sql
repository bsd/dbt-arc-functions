{% macro create_stg_stitch_sfmc_audience_transaction_jobs_append(
    reference_name='stg_stitch_sfmc_audience_transactions_join'
    , client=''
) %}

-- TODO: prompt user to enter client name if applicable during create or update set up?

-- this statement is meant to be unique for every date, transaction_id, and person_id 
-- with the intent of joining it back to the transaction table


{% if client == 'uusa' %}
    -- This SQL statement will be used if 'variable' has a value
    select 
    transaction_date,
    person_id,
    case 
    when cumulative_amount_12_months >= 25000 then 'major'
    when cumulative_amount_24months between 1000 and 24999 
        and cumulative_amount_12_months < 25000
    then 'midlevel'
    when cumulative_amount_30_days_recur > 0   
        and cumulative_amount_24months < 1000
        and cumulative_amount_12_months < 25000 then 'recurring'
        then 'recurring' 
    when cumulative_amount between 1 and 999 then 'grassroots'
    else null end as audience_type,
    -- new_donor definition missing for UUSA
    case 
    when donated_within_14_months = 0 
    then 'lapsed'
    -- active
    -- new donor
    -- reinstated
    -- multi-year 
    -- existing
    else null end as loyalty_type
    from {{ reference_name }}
         
{% else %}
    -- This SQL statement will be used if 'variable' is empty or does not exist
    select 
    transaction_date,
    person_id,
    -- code out the audience_types
    case 
    when donated_this_year = 0 then 'lapsed'
    when donated_within_14_months = 1
         and new_donor = 0 then 'active'
    when donated_this_year = 1
        and (donated_two_years_ago = 1 or donated_three_years_ago = 1)
        and donated_last_year = 0 then 'reinstated'
   -- multi-year donor
   when donated_this_year = 1
        and donated_last_year = 1
        then 'existing' -- this isn't exclusive from active!!
    when donated_this_year = 1
        and new_donor = 1 then 'new_donor'

    from {{ reference_name }}
 
{% endif %}


{% endmacro %}