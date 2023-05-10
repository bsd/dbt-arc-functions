{% macro create_stg_frakture_everyaction_email_campaign_dates(
    reference_name="stg_frakture_everyaction_email_summary_unioned"
) %}
    select
        safe_cast(publish_date as timestamp) as campaign_timestamp,
        safe_cast(campaign_label as string) as crm_campaign,
        safe_cast(campaign as string) as source_code_campaign
    from {{ ref(reference_name) }}
{% endmacro %}
