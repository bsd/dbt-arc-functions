{% macro create_stg_stitch_facebook_paidmedia_transactions_sourced_rollup(
    source_name="src_stitch_facebook_paidmedia",
    source_table="ads_insights",
    extract_fields="stg_stitch_facebook_paidmedia_extract_fields_from_adcreative_parameterized"
) %}
select
    ad_summary.ad_id as message_id,
    safe_cast(date_start as timestamp) as date_timestamp,
    cast(null as int) as total_revenue,
    cast(null as int) as total_gifts,
    cast(null as int) as total_donors,
    cast(null as int) as one_time_revenue,
    cast(null as int) as one_time_gifts,
    cast(null as int) as new_monthly_revenue,
    cast(null as int) as new_monthly_gifts,
    cast(null as int) as total_monthly_revenue,
    cast(null as int) as total_monthly_gifts,
    extract_fields.objective as objective,
    extract_fields.campaign as campaign,
    cast(null as string) as campaign_label,
    extract_fields.audience as audience,
    cast(null as string) as appeal,
    extract_fields.source_code_single as source_code,
from {{ source(source_name, source_table) }} ad_summary
left join
    {{ ref(extract_fields) }} extract_fields on ad_summary.ad_id = extract_fields.ad_id
{% endmacro %}
