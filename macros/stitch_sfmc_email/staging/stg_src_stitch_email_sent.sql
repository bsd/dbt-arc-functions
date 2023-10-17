{% macro create_stg_src_stitch_email_sent() %}
with sent_historical as (

    select
        subscriberkey as subscriber_key,
        domain as domain,
        triggerersenddefinitionobjectid as triggerrer_send_definition_object_id,
        triggeredsendcustomerkey as triggered_send_customer_key,
        CAST(accountid as INT64) as account_id,
        CAST(oybaccountid as INT64) as oyb_account_id,
        CAST(jobid as INT64) as job_id,
        CAST(listid as INT64) as list_id,
        CAST(batchid as INT64) as batch_id,
        CAST(subscriberid as INT64) as subscriber_id,
        DATETIME(
            CAST(
                CONCAT(
                    SUBSTR(eventdate, 0, 22), " America/New_York"
                ) as TIMESTAMP
            ),
            "America/New_York"
        ) as event_dt

    from {{ source('src_uusa_sfmc_historical','backfill_sent') }}

),

sent_stitch as (

    select
        subscriberkey as subscriber_key,
        domain,
        triggerersenddefinitionobjectid as triggerrer_send_definition_object_id,
        triggeredsendcustomerkey as triggered_send_customer_key,
        CAST(__accountid_ as INT64) as account_id,
        CAST(oybaccountid as INT64) as oyb_account_id,
        CAST(jobid as INT64) as job_id,
        CAST(listid as INT64) as list_id,
        CAST(batchid as INT64) as batch_id,
        CAST(subscriberid as INT64) as subscriber_id,
        DATETIME(
            CAST(
                CONCAT(
                    SUBSTR(eventdate, 0, 22), " America/New_York"
                ) as TIMESTAMP
            ),
            "America/New_York"
        ) as event_dt

    from {{ source('stitch_sfmc_email', 'sent') }}

),

sent_unioned as (

    select * from sent_historical
    union all
    select * from sent_stitch

)

select distinct * from sent_unioned where job_id is not null

{% endmacro %}
