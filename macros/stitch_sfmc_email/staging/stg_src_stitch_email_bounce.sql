{% macro create_stg_src_stitch_email_bounce() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^bounce$",
        is_source=True,
        source_name="stitch_sfmc_email",
        schema_to_search="src_stitch_sfmc_authorized",
    ) %}

    select distinct
        cast(__accountid_ as int64) as account_id,
        cast(oybaccountid as int64) as oyb_account_id,
        cast(jobid as int64) as job_id,
        cast(listid as int64) as list_id,
        cast(batchid as int64) as batch_id,
        cast(subscriberid as int64) as subscriber_id,
        subscriberkey as subscriber_key,
        datetime(
            cast(concat(substr(eventdate, 0, 22), " America/New_York") as timestamp),
            "America/New_York"
        ) as event_dt,
        cast(isunique as bool) as is_unique,
        domain as domain,
        cast(bouncecategoryid as string) as bounce_category_id,
        bouncecategory as bounce_category,
        bouncesubcategoryid as bounce_subcategory_id,
        bouncesubcategory as bounce_subcategory,
        cast(bouncetypeid as string) as bounce_type_id,
        bouncetype as bounce_type,
        cast(smtpcode as string) as smtp_code,
        triggerersenddefinitionobjectid as triggerrer_send_definition_object_id,
        cast(triggeredsendcustomerkey as string) as triggered_send_customer_key,
        _sdc_received_at as recieved_at
    from ({{ dbt_utils.union_relations(relations) }})

{% endmacro %}
