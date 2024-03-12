{% macro create_stg_stitch_google_paidmedia_leads_daily_rollup_by_country(
    source_name="src_stitch_googleads_paidmedia_by_country",
    source_table="geo_performance_conversion"
) %}

    select distinct
        cast(ad_group_id as string) as message_id,
        cast(date as timestamp) as date_timestamp,
        geographic_view_country_criterion_id as country,
        sum(case when conversion_action_category = 'SIGNUP' THEN 1 ELSE 0 END) AS leads
    from {{ source(source_name, source_table) }}
    where campaign_status = 'ENABLED'
{% endmacro %}
