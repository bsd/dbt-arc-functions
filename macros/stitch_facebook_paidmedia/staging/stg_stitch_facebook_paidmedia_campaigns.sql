{% macro create_stg_stitch_facebook_paidmedia_campaigns(
    source_name="src_stitch_facebook_paidmedia",
    source_table="ads_insights"
) %}
    select
        campaign_id as campaign_id,
        ad_id as message_id,
        'social' as channel_category,
        'facebook' as channel,
        null as channel_type,
        campaign_name as campaign_name,
        null as crm_entity,
        null as source_code_entity,
        null as preview_url
    from {{ source(source_name, source_table) }}
{% endmacro %}
