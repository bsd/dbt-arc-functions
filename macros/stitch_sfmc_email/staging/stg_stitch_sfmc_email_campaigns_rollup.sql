{% macro create_stg_stitch_sfmc_email_campaigns_rollup(
    reference_name="stg_stitch_sfmc_email_campaigns"
) %}
select distinct
    safe_cast(message_id as string) as message_id, -- just being extra safe since this is joining field
    crm_entity,
    source_code_entity,
    audience,
    recurtype,
    campaign_category,
    coalesce(crm_campaign, source_code_campaign) as campaign_name
from {{ ref(reference_name) }}
{% endmacro %}
