{% macro create_stg_paidmedia_actuals_campaign_jobs(
    reference_name='mart_paidmedia_daily_revenue_performance') %}
SELECT
distinct(lower(campaign_name)) as campaign_name,
channel,
channel_category,
channel_type,
objective

FROM {{ref(reference_name)}} 
WHERE campaign_name is not null

{% endmacro %}