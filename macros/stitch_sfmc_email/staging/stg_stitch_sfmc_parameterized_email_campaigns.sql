{% macro create_stg_stitch_sfmc_parameterized_email_campaigns(
    audience,
    source_code_campaign,
    reference_name="stg_src_stitch_email_job"
 %}

    select distinct
        safe_cast(job_id as string) as message_id,
        safe_cast('sfmc' as string) as crm_entity,
        safe_cast(null as string) as source_code_entity,
        {{ audience }} as audience,
        safe_cast(null as string) as recurtype,
        safe_cast(null as string) as campaign_category,
        safe_cast(null as string) as crm_campaign,
        {{source_code_campaign}} as source_code_campaign

    from {{ ref(reference_name) }}

{% endmacro %}
