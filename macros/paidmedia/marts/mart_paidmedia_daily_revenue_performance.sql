{% macro create_mart_paidmedia_daily_revenue_performance(
    campaigns="stg_paidmedia_campaigns_unioned",
    impressions="stg_paidmedia_impressions_daily_rollup_unioned",
    clicks="stg_paidmedia_clicks_daily_rollup_unioned",
    spends="stg_paidmedia_spends_daily_rollup_unioned",
    transactions="stg_paidmedia_transactions_sourced_daily_rollup_unioned",
    subscribes="stg_paidmedia_subscribes_daily_rollup_unioned"
) %}
select
    coalesce(campaigns.message_id, impressions.message_id) as message_id,
    campaigns.campaign_id,
    coalesce(
        campaigns.channel,
        regexp_extract(
            impressions._dbt_source_relation, 'stg_[a-z]+_([a-z_]+)_paidmedia'
        )
    ) as channel,
    campaigns.channel_category,
    campaigns.channel_type,
    campaigns.campaign_name,
    campaigns.crm_entity,
    campaigns.source_code_entity,
    case
        when
            (
                campaigns.crm_entity is not null
                and campaigns.source_code_entity is not null
            )
        then concat(campaigns.crm_entity, '-', campaigns.source_code_entity)
        else coalesce(campaigns.crm_entity, campaigns.source_code_entity)
    end as best_guess_entity,
    campaigns.preview_url,
    impressions.date_timestamp,
    impressions.total_impressions,
    impressions.unique_impressions,
    clicks.total_clicks,
    clicks.unique_clicks,
    spend.spend_amount,
    transactions.total_revenue,
    transactions.total_gifts,
    transactions.total_donors,
    transactions.one_time_revenue,
    transactions.one_time_gifts,
    transactions.new_monthly_revenue,
    transactions.new_monthly_gifts,
    transactions.total_monthly_revenue,
    transactions.total_monthly_gifts,
    transactions.objective,
    transactions.campaign,
    transactions.campaign_label,
    transactions.audience,
    transactions.appeal,
    transactions.source_code,
    subscribes.subscribes
from {{ ref(campaigns) }} campaigns
full join
    {{ ref(impressions) }} impressions on campaigns.message_id = impressions.message_id
full join
    {{ ref(clicks) }} clicks
    on impressions.message_id = clicks.message_id
    and impressions.date_timestamp = clicks.date_timestamp
full join
    {{ ref(spends) }} spend
    on impressions.message_id = spend.message_id
    and impressions.date_timestamp = spend.date_timestamp
full join
    {{ ref(transactions) }} transactions
    on impressions.message_id = transactions.message_id
    and impressions.date_timestamp = transactions.date_timestamp
full join
    {{ ref(subscribes) }} subscribes
    on impressions.message_id = subscribes.message_id
    and impressions.date_timestamp = subscribes.date_timestamp
{% endmacro %}
