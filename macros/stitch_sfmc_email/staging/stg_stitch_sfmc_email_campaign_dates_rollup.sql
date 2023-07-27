{% macro create_stg_stitch_sfmc_email_campaign_dates_rollup(
    reference_name="stg_stitch_sfmc_customized_email_campaign_dates"
) %}
    select
        coalesce(crm_campaign, source_code_campaign) as campaign_name,
        min(campaign_timestamp) as campaign_start_timestamp,
        max(campaign_timestamp) as campaign_latest_timestamp
    from {{ ref(reference_name) }}
    group by 1
{% endmacro %} xs
