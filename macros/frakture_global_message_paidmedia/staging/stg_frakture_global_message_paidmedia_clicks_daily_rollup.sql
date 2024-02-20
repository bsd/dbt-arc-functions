{% macro create_stg_frakture_global_message_paidmedia_clicks_daily_rollup(
    reference_name="stg_frakture_global_message_paidmedia_ad_summary_by_date"
) %}
    select distinct
        safe_cast(message_id as string) as message_id,
        safe_cast(ad_summary_by_date.date as timestamp) as date_timestamp,
        safe_cast(ad_summary_by_date.clicks as int) as total_clicks,
        safe_cast(ad_summary_by_date.clicks as int) as unique_clicks
    from {{ ref(reference_name) }} ad_summary_by_date
{% endmacro %}
