{% macro create_stg_stitch_sfmc_audience_transaction_jobs_append(
    reference_name='stg_stitch_sfmc_audience_transaction_jobs'
    , client=''
) %}

-- TODO: prompt user to enter client name if applicable during create or update set up?

{% if client == 'uusa' %}
    -- This SQL statement will be used if 'variable' has a value

{% else %}
    -- This SQL statement will be used if 'variable' is empty or does not exist
 
{% endif %}


{% endmacro %}