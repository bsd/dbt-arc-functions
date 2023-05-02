{% macro create_stg_stitch_sfmc_email_campaign_dates_rollup(
    reference_name="stg_stitch_sfmc_email_campaign_dates"
) %}

{% if var.database == 'bsd-arc-uusa' %}

select
    source_code_campaign as campaign_name,
    min(campaign_timestamp) as campaign_start_timestamp,
    max(campaign_timestamp) as campaign_latest_timestamp
from {{ ref(reference_name) }}
group by 1


{% else %}



select
    coalesce(source_code_campaign, crm_campaign) as campaign_name,
    min(campaign_timestamp) as campaign_start_timestamp,
    max(campaign_timestamp) as campaign_latest_timestamp
from {{ ref(reference_name) }}
group by 1

    
{% endif %}

{% endmacro %}