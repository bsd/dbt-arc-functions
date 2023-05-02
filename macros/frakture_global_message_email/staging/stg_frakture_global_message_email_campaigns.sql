{% macro create_stg_frakture_global_message_email_campaigns(
    reference_name="stg_frakture_global_message_email_summary"
) %}
    select distinct
        safe_cast(message_id as string) as message_id,
        safe_cast(bot_nickname as string) as crm_entity,
        safe_cast(account_prefix as string) as source_code_entity,
        safe_cast(audience as string) as audience,
        safe_cast(recurtype as string) as recurtype,
        safe_cast(message_set as string) as campaign_category,
        safe_cast(campaign_name as string) as crm_campaign,
        safe_cast(coalesce(campaign, campaign_label) as string) as source_code_campaign
    from {{ ref(reference_name) }}

{% endmacro %}
