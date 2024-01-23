{% macro create_stg_src_stitch_email_job() %}

with
    deduplicated_data as (
        select distinct
            cast(__jobid_ as int64) as job_id,
            cast(emailid as int64) as email_id,
            cast(accountid as int64) as account_id,
            cast(accountuserid as int64) as account_user_id,
            fromname as from_name,
            fromemail as from_email,
            (
                case
                    when length(schedtime) > 0
                    then
                        datetime(
                            cast(
                                concat(
                                    substr(schedtime, 0, 22), " America/New_York"
                                ) as timestamp
                            ),
                            "America/New_York"
                        )
                    else null
                end
            ) as sched_dt,
            (
                case
                    when length(pickuptime) > 0
                    then
                        datetime(
                            cast(
                                concat(
                                    substr(pickuptime, 0, 22), " America/New_York"
                                ) as timestamp
                            ),
                            "America/New_York"
                        )
                    else null
                end
            ) as pickup_dt,
            (
                case
                    when length(deliveredtime) > 0
                    then
                        datetime(
                            cast(
                                concat(
                                    substr(deliveredtime, 0, 22), " America/New_York"
                                ) as timestamp
                            ),
                            "America/New_York"
                        )
                    else null
                end
            ) as delivered_dt,
            eventid as event_id,
            cast(ismultipart as bool) as is_multipart,
            jobtype as job_type,
            jobstatus as job_status,
            cast(modifiedby as string) as modified_by,
            (
                case
                    when length(modifieddate) > 0
                    then
                        datetime(
                            cast(
                                concat(
                                    substr(modifieddate, 0, 22), " America/New_York"
                                ) as timestamp
                            ),
                            "America/New_York"
                        )
                    else null
                end
            ) as modified_dt,
            emailname as email_name,
            emailsubject as email_subject,
            cast(iswrapped as bool) as is_wrapped,
            testemailaddr as test_email_addr,
            category as category,
            bccemail as bcc_email,
            originalschedtime as original_sched_time,
            (
                case
                    when length(createddate) > 0
                    then
                        datetime(
                            cast(
                                concat(
                                    substr(createddate, 0, 22), " America/New_York"
                                ) as timestamp
                            ),
                            "America/New_York"
                        )
                    else null
                end
            ) as created_dt,
            characterset as character_set,
            ipaddress as ip_address,
            cast(
                salesforcetotalsubscribercount as int64
            ) as salesforce_total_subscriber_count,
            cast(
                salesforceerrorsubscribercount as int64
            ) as salesforce_error_subscriber_count,
            sendtype as send_type,
            dynamicemailsubject as dynamic_email_subject,
            cast(suppresstracking as bool) as suppress_tracking,
            sendclassificationtype as send_classification_type,
            sendclassification as send_classification,
            cast(
                resolvelinkswithcurrentdata as bool
            ) as resolve_links_with_current_data,
            emailsenddefinition as email_send_definition,
            cast(deduplicatebyemail as bool) as deduplicated_by_email,
            triggerersenddefinitionobjectid as triggerrer_send_definition_object_id,
            triggeredsendcustomerkey as triggered_send_customer_key,
            row_number() over (
                partition by __jobid_
                order by schedtime desc, pickuptime desc, deliveredtime desc
            ) as row_num
        from {{ source("stitch_sfmc_email", "job") }}
        where __jobid_ is not null
    )

select distinct
    job_id,
    email_id,
    account_id,
    account_user_id,
    from_name,
    from_email,
    sched_dt,
    pickup_dt,
    delivered_dt,
    event_id,
    is_multipart,
    job_type,
    job_status,
    modified_by,
    modified_dt,
    email_name,
    email_subject,
    is_wrapped,
    test_email_addr,
    category,
    bcc_email,
    original_sched_time,
    created_dt,
    character_set,
    ip_address,
    salesforce_total_subscriber_count,
    salesforce_error_subscriber_count,
    send_type,
    dynamic_email_subject,
    suppress_tracking,
    send_classification_type,
    send_classification,
    resolve_links_with_current_data,
    email_send_definition,
    deduplicated_by_email,
    triggerrer_send_definition_object_id,
    triggered_send_customer_key
from deduplicated_data
where row_num = 1

{% endmacro %}
