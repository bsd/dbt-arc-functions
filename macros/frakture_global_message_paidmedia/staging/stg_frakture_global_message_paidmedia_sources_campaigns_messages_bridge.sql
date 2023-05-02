{% macro create_stg_frakture_global_message_paidmedia_sources_campaigns_messages_bridge(
    reference_name="stg_frakture_global_message_paidmedia_ad_summary_by_date"
) %}
    select distinct
        safe_cast(message_id as string) as message_id,
        safe_cast(primary_source_code as string) as source_code,
        safe_cast(campaign_id as string) as campaign_id
    from {{ ref(reference_name) }}
{% endmacro %}
