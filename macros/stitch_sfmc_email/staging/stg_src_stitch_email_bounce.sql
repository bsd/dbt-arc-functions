{% macro create_stg_src_stitch_email_bounce() %}

    with
        deduplicated_data as (
            select
                cast(__accountid_ as int64) as account_id,
                cast(oybaccountid as int64) as oyb_account_id,
                cast(jobid as int64) as job_id,
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
                cast(isunique as bool) as is_unique,
                domain,
                cast(bouncecategoryid as string) as bounce_category_id,
                bouncecategory as bounce_category,
                bouncesubcategoryid as bounce_subcategory_id,
                bouncesubcategory as bounce_subcategory,
                cast(bouncetypeid as string) as bounce_type_id,
                bouncetype as bounce_type,
                cast(smtpcode as string) as smtp_code,
                triggerersenddefinitionobjectid as triggerrer_send_definition_object_id,
                cast(triggeredsendcustomerkey as string) as triggered_send_customer_key,
                _sdc_received_at as recieved_at,
                row_number() over (partition by subscriberkey order by eventdate) as row_num
            from {{ source("stitch_sfmc_email", "bounce") }}
            where jobid is not null
        )

    select
        account_id,
        oyb_account_id,
        job_id,
        list_id,
        batch_id,
        subscriber_id,
        subscriber_key,
        event_dt,
        is_unique,
        domain,
        bounce_category_id,
        bounce_category,
        bounce_subcategory_id,
        bounce_subcategory,
        bounce_type_id,
        bounce_type,
        smtp_code,
        triggerrer_send_definition_object_id,
        triggered_send_customer_key,
        recieved_at
    from deduplicated_data
    where row_num = 1

{% endmacro %}
