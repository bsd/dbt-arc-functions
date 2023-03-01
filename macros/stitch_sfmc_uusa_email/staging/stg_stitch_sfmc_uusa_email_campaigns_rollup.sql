null{% macro create_stg_stitch_sfmc_uusa_email_campaigns_rollup(
    reference_name='stg_stitch_sfmc_uusa_email_campaigns') %}
SELECT DISTINCT message_id,
    crm_entity,
    source_code_entity,
    audience,
    recurtype,
    campaign_category,
    COALESCE(crm_campaign,source_code_campaign) AS campaign_name
FROM {{ ref(reference_name) }}
{% endmacro %}