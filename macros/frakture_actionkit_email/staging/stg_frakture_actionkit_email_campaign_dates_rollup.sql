{% macro create_stg_frakture_actionkit_email_campaign_dates_rollup(
    reference_name="stg_frakture_actionkit_email_campaign_dates"
) %}
select
    coalesce(crm_campaign, source_code_campaign) as campaign_name,
    min(campaign_timestamp) as campaign_start_timestamp,
    max(campaign_timestamp) as campaign_latest_timestamp
from {{ ref(reference_name) }}
group by 1
{% endmacro %}
