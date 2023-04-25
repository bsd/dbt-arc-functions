{% macro create_stg_frakture_global_message_paidmedia_campaigns(
    reference_name="stg_frakture_global_message_paidmedia_message"
) %}
    select distinct
        safe_cast(campaign_id as string) as campaign_id,
        safe_cast(message_id as string) as message_id,
        case
            when regexp_contains(type, '(?i)search') = true
            then 'search'
            when regexp_contains(type, '(?i)ad') = true
            then 'search'
            when regexp_contains(type, '(?i)display') = true
            then 'display'
            when regexp_contains(type, '(?i)video') = true
            then 'display'
            when channel in ('ad')
            then 'search'
            when channel in ('facebook_ad', 'promoted_tweet')
            then 'social'
            else concat(channel, ' - ', type)
        end as channel_category,
        safe_cast(channel as string) as channel,
        safe_cast(type as string) as channel_type,
        safe_cast(campaign_name as string) as campaign_name,
        safe_cast(bot_id as string) as crm_entity,
        safe_cast(account_prefix as string) as source_code_entity,
        safe_cast(preview_url as string) as preview_url
    from {{ ref(reference_name) }}
{% endmacro %}
