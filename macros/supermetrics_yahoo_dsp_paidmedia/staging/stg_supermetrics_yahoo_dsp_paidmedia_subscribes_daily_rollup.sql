{% macro create_stg_supermetrics_yahoo_dsp_paidmedia_subscribes_daily_rollup(
    source_name="supermetrics_yahoo_dsp_paidmedia",
    source_table_name="alldates_VDSP_AD"
) %}
    select distinct
        safe_cast(ad_summary_by_date.ad_id as string) as message_id,
        safe_cast(ad_summary_by_date.date as timestamp) as date_timestamp,
        safe_cast(null as integer) as subscribes
    from {{ source(source_name, source_table_name) }} ad_summary_by_date
{% endmacro %}
