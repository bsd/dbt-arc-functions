{% macro create_stg_src_stitch_email_unsubscribe() %}
{% set relations= dbt_arc_functions.relations_that_match_regex('^unsubscribe$')
    is_source=True,
  source_name='stitch_sfmc_email',
  schema_to_search='src_stitch_sfmc_authorized' %}


Select DISTINCT
        CAST(accountid AS INT64) as account_id
        ,CAST(__oybaccountid_ AS INT64) as oyb_account_id
        ,CAST(jobid AS INT64) as job_id
        ,CAST(listid AS INT64) as list_id
        ,CAST(batchid AS INT64) as batch_id
        ,CAST(subscriberid AS INT64) as subscriber_id
        ,subscriberkey as subscriber_key
        ,datetime(CAST(CONCAT(Substr(eventdate,0,22)," America/New_York") as timestamp), "America/New_York") as event_dt
        ,CAST(isunique AS BOOL) as is_unique
        ,domain
    from {{ref(relations)}}

{% endmacro %}