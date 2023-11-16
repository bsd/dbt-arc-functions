{% macro create_stg_stitch_facebook_paidmedia_campaigns(
    source_name="src_stitch_facebook_paidmedia",
    source_table="ads_insights"
) %}
    select distinct
        campaign_id as campaign_id,
        ad_id as message_id,
        'social' as channel_category,
        'facebook_ad' as channel,
        cast(null as string) as channel_type,
        campaign_name as campaign_name,
        cast(null as string) as crm_entity,
        cast(null as string) as source_code_entity,
        cast(null as string) as preview_url
    from {{ source(source_name, source_table) }}
{% endmacro %}
