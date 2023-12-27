{% macro create_stg_stitch_sfmc_parameterized_email_campaign_dates(
    reference_name="stg_src_stitch_email_job", source_code_campaign="NULL"
) %}

    -- reference campaigns model so that no repetition is necessary in case when for
    -- campaigns
    select
        safe_cast(coalesce(sched_dt, pickup_dt) as timestamp) as campaign_timestamp,
        safe_cast(null as string) as crm_campaign,
        safe_cast({{ source_code_campaign }} as string) as source_code_campaign
    from {{ ref(reference_name) }}
{% endmacro %}
