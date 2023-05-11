{% macro create_stg_stitch_sfmc_email_campaign_dates_rollup(
    reference_name="stg_stitch_sfmc_email_campaign_dates"
) %}

    {% if var.database == "bsd-arc-uusa" %}

        select
            safe_cast(source_code_campaign as string) as campaign_name,
            safe_cast(min(campaign_timestamp) as timestamp) as campaign_start_timestamp,
            safe_cast(max(campaign_timestamp) as timestamp) as campaign_latest_timestamp
        from {{ ref(reference_name) }}
        group by 1

    {% else %}

        select
            safe_cast(
                coalesce(source_code_campaign, crm_campaign) as string
            ) as campaign_name,
            safe_cast(min(campaign_timestamp) as timestamp) as campaign_start_timestamp,
            safe_cast(max(campaign_timestamp) as timestamp) as campaign_latest_timestamp
        from {{ ref(reference_name) }}
        group by 1

    {% endif %}

{% endmacro %}
