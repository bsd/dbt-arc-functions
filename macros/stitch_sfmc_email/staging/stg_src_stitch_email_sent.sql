{% macro create_stg_src_stitch_email_sent() %}
    with
        sent_historical as (

            select
                subscriberkey as subscriber_key,
                domain as domain,
                triggerersenddefinitionobjectid as triggerrer_send_definition_object_id,
                triggeredsendcustomerkey as triggered_send_customer_key,
                cast(accountid as int64) as account_id,
                cast(oybaccountid as int64) as oyb_account_id,
                cast(jobid as int64) as job_id,
                cast(listid as int64) as list_id,
                cast(batchid as int64) as batch_id,
                cast(subscriberid as int64) as subscriber_id,
                datetime(
                    cast(
                        concat(
                            substr(eventdate, 0, 22), " America/New_York"
                        ) as timestamp
                    ),
                    "America/New_York"
                ) as event_dt

            from {{ source("src_uusa_sfmc_historical", "backfill_sent") }}

        ),

        sent_stitch as (

            select
                subscriberkey as subscriber_key,
                lower(domain) as domain,
                triggerersenddefinitionobjectid as triggerrer_send_definition_object_id,
                triggeredsendcustomerkey as triggered_send_customer_key,
                cast(__accountid_ as int64) as account_id,
                cast(oybaccountid as int64) as oyb_account_id,
                cast(jobid as int64) as job_id,
                cast(listid as int64) as list_id,
                cast(batchid as int64) as batch_id,
                cast(subscriberid as int64) as subscriber_id,
                datetime(
                    cast(
                        concat(
                            substr(eventdate, 0, 22), " America/New_York"
                        ) as timestamp
                    ),
                    "America/New_York"
                ) as event_dt

            from {{ source("stitch_sfmc_email", "sent") }}

        ),

        sent_unioned as (

            select *
            from sent_historical
            union all
            select *
            from sent_stitch

        )

    select distinct *
    from sent_unioned
    where job_id is not null

{% endmacro %}
