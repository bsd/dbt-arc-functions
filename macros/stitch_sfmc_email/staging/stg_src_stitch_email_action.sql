{% macro create_stg_src_stitch_email_action() %}

    select
        cast(0 as int64) as account_id,
        cast(0 as int64) as oyb_account_id,
        cast(0 as int64) as job_id,
        cast(0 as int64) as list_id,
        cast(0 as int64) as batch_id,
        cast(0 as int64) as subscriber_id,
        '' as subscriber_key,
        cast(null as datetime) as event_dt,
        cast('' as string) as domain,
        cast('' as string) as url,
        cast('' as string) as link_name,
        cast('' as string) as link_content,
        cast(null as bool) as is_unique,
        cast(null as string) as triggerrer_send_definition_object_id,
        cast(null as string) as triggered_send_customer_key,
    from {{ source("stitch_sfmc_email", "click") }}  -- is click because client does not have action
    where jobid is not null

{% endmacro %}
