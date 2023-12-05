{% macro create_stg_stitch_google_paidmedia_campaigns(
    source_name="src_stitch_googleads_paidmedia", source_table="ad_performance_report"
) %}
select distinct
    campaign_id as campaign_id,
    id as message_id,
    'search' as channel_category,
    'google_ad' as channel,
    cast(null as string) as channel_type,
    campaign_name as campaign_name,
    cast(null as string) as crm_entity,
    cast(json_extract_scalar(final_urls, '$[0]') as string) as preview_url,
    case
        when json_extract_scalar(final_urls, '$[0]') is not null
            then
                regexp_extract(
                    json_extract_scalar(final_urls, '$[0]'),
                    'cartId=([^&]*)'
                )
        else
            'No Source Code'
    end as source_code_entity
from {{ source(source_name, source_table ) }}

{% endmacro %}