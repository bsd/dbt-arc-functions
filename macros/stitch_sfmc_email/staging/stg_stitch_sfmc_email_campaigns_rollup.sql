null
{% macro create_stg_stitch_sfmc_email_campaigns_rollup(
    reference_name="stg_stitch_sfmc_customized_email_campaigns"
) %}
    select distinct
        message_id,
        crm_entity,
        source_code_entity,
        audience,
        recurtype,
        campaign_category,
        coalesce(crm_campaign, source_code_campaign) as campaign_name
    from {{ ref(reference_name) }}
{% endmacro %}
