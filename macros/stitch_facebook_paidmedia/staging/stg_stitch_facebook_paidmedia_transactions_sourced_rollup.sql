{% macro create_stg_stitch_facebook_paidmedia_transactions_sourced_rollup(
    source_name="src_stitch_facebook_paidmedia",
    source_table="ads_insights"
) %}
    select
        null as message_id,
        null as date_timestamp,
        null as total_revenue,
        null as total_gifts,
        null as total_donors,
        null as one_time_revenue,
        null as one_time_gifts,
        null as new_monthly_revenue,
        null as new_monthly_gifts,
        null as total_monthly_revenue,
        null as total_monthly_gifts,
        null as objective,
        null as campaign,
        null as campaign_label,
        null as audience,
        null as appeal,
        null as source_code,
    from {{ source(source_name, source_table) }} ad_summary
{% endmacro %}
