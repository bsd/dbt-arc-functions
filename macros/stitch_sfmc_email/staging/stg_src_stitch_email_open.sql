{% macro create_stg_src_stitch_email_open() %}
{% set relations= dbt_arc_functions.relations_that_match_regex('^open$',
    is_source=True,
  source_name='stitch_sfmc_email',
  schema_to_search='src_stitch_sfmc_authorized') %}



select DISTINCT
        CAST(__accountid_ AS INT64) as account_id
        ,CAST(oybaccountid AS INT64) as oyb_account_id
        ,CAST(jobid AS INT64) as job_id
        ,CAST(listid AS INT64) as list_id
        ,CAST(batchid AS INT64) as batch_id
        ,CAST(subscriberid AS INT64) as subscriber_id
        ,subscriberkey as subscriber_key
        ,datetime(CAST(CONCAT(Substr(eventdate,0,22)," America/New_York") as timestamp), "America/New_York") as event_dt
        , domain
        ,CAST(isunique AS BOOL) as is_unique
        ,triggerersenddefinitionobjectid as triggerrer_send_definition_object_id
        ,CAST(triggeredsendcustomerkey as STRING) as triggered_send_customer_key

    from {{ref(relations)}}

{% endmacro %}
