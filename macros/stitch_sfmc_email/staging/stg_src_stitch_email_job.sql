{% macro create_stg_src_stitch_email_job() %}
{% set relations= dbt_arc_functions.relations_that_match_regex('^job$'),
    is_source=True,
  source_name='stitch_sfmc_email',
  schema_to_search='src_stitch_sfmc_authorized' %}

SELECT distinct
    CAST(__jobid_ AS INT64) as job_id
    ,CAST(emailid AS INT64) as email_id
    ,CAST(accountid AS INT64) as account_id
    ,CAST(accountuserid AS INT64) as account_user_id
    ,fromname as from_name
    ,fromemail as from_email
    ,( case
        when length(schedtime) > 0 then datetime(CAST(CONCAT(Substr(schedtime,0,22)," America/New_York") as timestamp), "America/New_York")
        else null
    end ) as sched_dt
    ,( case
        when length(pickuptime) > 0 then datetime(CAST(CONCAT(Substr(pickuptime,0,22)," America/New_York") as timestamp), "America/New_York")
        else null
    end ) as pickup_dt
    ,( case
        when length(deliveredtime) > 0 then datetime(CAST(CONCAT(Substr(deliveredtime,0,22)," America/New_York") as timestamp), "America/New_York")
        else null
    end ) as delivered_dt
    ,eventid as event_id
    ,CAST(ismultipart AS BOOL) as is_multipart
    ,jobtype as job_type
    ,jobstatus as job_status
    ,CAST(modifiedby as STRING) as modified_by
    ,(case
        when length(modifieddate) > 0 then datetime(CAST(CONCAT(Substr(modifieddate,0,22)," America/New_York") as timestamp), "America/New_York")
        else null
    end ) as modified_dt
    ,emailname as email_name
    ,emailsubject as email_subject
    ,CAST(iswrapped AS BOOL) as is_wrapped
    ,testemailaddr as test_email_addr
    ,category as category
    ,bccemail as bcc_email
    ,originalschedtime as original_sched_time
    ,( case
        when length(createddate) > 0 then datetime(CAST(CONCAT(Substr(createddate,0,22)," America/New_York") as timestamp), "America/New_York")
        else null
    end ) as created_dt
    ,characterset as character_set
    ,ipaddress as ip_address
    ,CAST(salesforcetotalsubscribercount AS INT64) as salesforce_total_subscriber_count
    ,CAST(salesforceerrorsubscribercount AS INT64) as salesforce_error_subscriber_count
    ,sendtype as send_type
    ,dynamicemailsubject as dynamic_email_subject
    ,CAST(suppresstracking AS BOOL) as suppress_tracking
    ,sendclassificationtype as send_classification_type
    ,sendclassification as send_classification
    ,CAST(resolvelinkswithcurrentdata AS BOOL) as resolve_links_with_current_data
    ,emailsenddefinition as email_send_definition
    ,CAST(deduplicatebyemail AS BOOL) as deduplicated_by_email
    ,triggerersenddefinitionobjectid as triggerrer_send_definition_object_id
    ,triggeredsendcustomerkey as triggered_send_customer_key
    from {{ref(relations)}}

{% endmacro %}