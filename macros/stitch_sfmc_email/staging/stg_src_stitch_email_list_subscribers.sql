{% macro create_stg_src_stitch_email_list_subscribers() %}
{% set relations= dbt_arc_functions.relations_that_match_regex('^list_subscribers$')}
    is_source=True,
  source_name='stitch_sfmc_email',
  schema_to_search='src_stitch_sfmc_authorized')
%}

SELECT distinct
    CAST(AddedBy AS INT64) as added_by
    ,AddMethod as add_method
    ,datetime(CAST(CONCAT(Substr(SAFE_CAST(CreatedDate AS STRING),0,22)," America/New_York") as timestamp), "America/New_York") as created_dt
    ,datetime(CAST(CONCAT(Substr(SAFE_CAST(DateUnsubscribed AS STRING),0,22)," America/New_York") as timestamp), "America/New_York") as unsubscribed_dt
    ,EmailAddress as email_address
    ,CAST(ListID AS INT64) as list_id
    ,ListName as list_name
    ,ListType as list_type
    ,Status as status
    ,CAST(SubscriberID AS INT64) as subscriber_id
    ,SubscriberKey as subscriber_key
    ,SubscriberType as subscriber_type
    from {{ref(relations)}}

{% endmacro %}
