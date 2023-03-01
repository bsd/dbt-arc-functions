{% macro create_stg_src_stitch_email_action() %}
{% set relations= dbt_arc_functions.relations_that_match_regex('^ftaf$') 
    is_source=True,
  source_name='stitch_sfmc_email',
  schema_to_search='src_stitch_sfmc_authorized' %}


SELECT distinct
    CAST(AccountID AS INT64) as account_id
    ,CAST(OYBAccountID AS INT64) as oyb_account_id
    ,CAST(JobID AS INT64) as job_id
    ,CAST(ListID AS INT64) as list_id
    ,CAST(BatchID AS INT64) as batch_id
    ,CAST(SubscriberID AS INT64) as subscriber_id
    ,SubscriberKey as subscriber_key
    ,datetime(CAST(CONCAT(Substr(TransactionTime,0,22)," America/New_York") as timestamp), "America/New_York") as transaction_dt
    ,CAST(IsUnique AS BOOL) as is_unique
    ,Domain as domain
    ,TriggererSendDefinitionObjectID as triggerrer_send_definition_object_id
    ,TriggeredSendCustomerKey as triggered_send_customer_key

    from {{ref(relations)}}

{% endmacro %}