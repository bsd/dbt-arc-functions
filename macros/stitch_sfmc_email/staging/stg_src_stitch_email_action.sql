{% macro create_stg_src_stitch_email_action() %}

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
                    domain,
                    url,
                    linkname as link_name,
                    linkcontent as link_content,
                    cast(isunique as bool) as is_unique,
                    triggerersenddefinitionobjectid
                    as triggerrer_send_definition_object_id,
                    triggeredsendcustomerkey as triggered_send_customer_key,
                    row_number() over (partition by jobid order by eventate) as row_num
                from {{ source("stitch_sfmc_email", "click") }}  -- is click because UUSA does not have action
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
            domain,
            url,
            link_name,
            link_content,
            is_unique,
            triggerrer_send_definition_object_id,
            triggered_send_customer_key
        from deduplicated_data
        where row_num = 1 

    {% endmacro %}
