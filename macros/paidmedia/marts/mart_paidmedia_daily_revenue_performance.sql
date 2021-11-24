{% macro create_mart_paidmedia_daily_revenue_performance(
    campaigns='stg_paidmedia_campaigns_unioned',
    impressions='stg_paidmedia_impressions_daily_rollup_unioned',
    clicks='stg_paidmedia_clicks_daily_rollup_unioned',
    spends='stg_paidmedia_spends_daily_rollup_unioned',
    transactions='stg_paidmedia_transactions_sourced_daily_rollup_unioned',
    subscribes='stg_paidmedia_subscribes_daily_rollup_unioned') %}
SELECT
      campaigns.message_id,
      campaigns.campaign_id,
      campaigns.channel,
      campaigns.campaign_name,
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
      subscribes.subscribes
FROM {{ ref(campaigns) }} campaigns
FULL JOIN {{ ref(impressions) }} impressions
  ON campaigns.message_id = impressions.message_id
FULL JOIN {{ ref(clicks) }} clicks
  ON impressions.message_id = clicks.message_id
  AND impressions.date_timestamp = clicks.date_timestamp
FULL JOIN {{ ref(spends) }} spend
  ON impressions.message_id = spend.message_id
  AND impressions.date_timestamp = spend.date_timestamp
FULL JOIN {{ ref(transactions) }} transactions
  ON impressions.message_id = transactions.message_id
  AND impressions.date_timestamp = transactions.date_timestamp
FULL JOIN {{ ref(subscribes) }} subscribes
  ON impressions.message_id = subscribes.message_id
  AND impressions.date_timestamp = subscribes.date_timestamp
{% endmacro %}