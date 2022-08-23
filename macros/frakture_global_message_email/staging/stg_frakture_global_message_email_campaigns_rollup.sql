{% macro create_stg_frakture_global_message_email_campaigns_rollup(
    reference_name='stg_frakture_global_message_email_campaigns') %}
SELECT DISTINCT message_id,
crm_entity,
source_code_entity,
audience,
case when crm_campaign is not null 
      then crm_campaign 
      else source_code_campaign END 
      AS campaign_name
FROM {{ ref(reference_name) }}

{% endmacro %}