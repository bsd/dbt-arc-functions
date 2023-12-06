{% macro create_stg_stitch_google_paidmedia_transactions_sourced_rollup(
    source_name="src_stitch_googleads_paidmedia", source_table="ad_performance_report"

) %}
   select
        cast(id as string) as message_id,
        cast(date as timestamp) as date_timestamp,
        cast(null as int) as total_revenue,
        cast(null as int) as total_gifts,
        cast(null as int) as total_donors,
        cast(null as int) as one_time_revenue,
        cast(null as int) as one_time_gifts,
        cast(null as int) as new_monthly_revenue,
        cast(null as int) as new_monthly_gifts,
        cast(null as int) as total_monthly_revenue,
        cast(null as int) as total_monthly_gifts,
        case when campaign_name like '%Appeal%' then 'donate'
        when campaign_name like '%Brand%' then 'brand'
        else null end as objective,
        campaign_name as campaign,
        cast(null as string) as campaign_label,
        cast(null as string) as audience,
        cast(null as string) as appeal,
        cast(null as string) as source_code,
 from  {{ source(source_name, source_table ) }}
{% endmacro %}