{% macro create_stg_frakture_global_message_email_campaign_dates(
    reference_name="stg_frakture_global_message_email_summary"
) %}
select
    safe_cast(publish_date as timestamp) as campaign_timestamp,
    safe_cast(campaign_name as string) as crm_campaign,
    safe_cast(coalesce(campaign, campaign_label) as string) as source_code_campaign
from {{ ref(reference_name) }}
{% endmacro %}
