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
    case when cumulative_amount between 1 and 999 then 'grassroots'
    when 

    
{% else %}
    -- This SQL statement will be used if 'variable' is empty or does not exist
 
{% endif %}


{% endmacro %}