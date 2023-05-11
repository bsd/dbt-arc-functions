{% macro create_stg_supermetrics_yahoo_dsp_paidmedia_campaigns(
    source_name="supermetrics_yahoo_dsp_paidmedia",
    source_table_name="alldates_VDSP_AD"
) %}
    select distinct
        safe_cast(ad.line_id as string) as campaign_id,
        safe_cast(ad.ad_id as string) as message_id,
        safe_cast('yahoo_dsp' as string) as channel,
        safe_cast(null as string) as channel_type,
        safe_cast(ad.line_name as string) as campaign_name
    from {{ source(source_name, source_table_name) }} ad
{% endmacro %}
