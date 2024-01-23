{% macro create_stg_stitch_facebook_paidmedia_campaigns(
    source_name="src_stitch_facebook_paidmedia", source_table="ads"
) %}
    select distinct
        ads.campaign_id as campaign_id,
        ads.ad_id as message_id,
        'social' as channel_category,
        'facebook_ad' as channel,
        cast(null as string) as channel_type,
        campaigns.name as campaign_name,
        cast(null as string) as crm_entity,
        cast(null as string) as source_code_entity,
        cast(null as string) as preview_url
    from {{ source(source_name, source_table) }} ads
    join {{ source(source_name, "campaigns") }} campaigns
        on ads.campaign_id = campaigns.id
{% endmacro %}
