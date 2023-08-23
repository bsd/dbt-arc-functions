{% macro create_stg_src_stitch_email_sent() %}

    with
        deduplicated_data as (
            select distinct
                cast(__accountid_ as int64) as account_id,
                cast(oybaccountid as int64) as oyb_account_id,
                cast(jobid as int64) as message_id,
                cast(listid as int64) as list_id,
                cast(batchid as int64) as batch_id,
                cast(subscriberid as int64) as subscriber_id,
                subscriberkey as subscriber_key,
                datetime(
                    cast(
                        concat(
                            substr(eventdate, 0, 22), " America/New_York"
                        ) as timestamp
                    ),
                    "America/New_York"
                ) as event_dt,
                domain,
                triggerersenddefinitionobjectid as triggerrer_send_definition_object_id,
                triggeredsendcustomerkey as triggered_send_customer_key,
                row_number() over (partition by subscriberkey order by eventdate) as row_num
            from {{ source("stitch_sfmc_email", "sent") }}
            where jobid is not null
        )

    select distinct
        account_id,
        oyb_account_id,
        message_id,
        list_id,
        batch_id,
        subscriber_id,
        subscriber_key,
        event_dt,
        domain,
        triggerrer_send_definition_object_id,
        triggered_send_customer_key
    from deduplicated_data
    where row_num = 1

{% endmacro %}
