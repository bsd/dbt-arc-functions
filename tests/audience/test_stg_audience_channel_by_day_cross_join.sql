SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT donor_audience) AS distinct_audiences,
  COUNT(DISTINCT channel) AS distinct_channels
FROM (
  SELECT
    date_day,
    donor_audience,
    channel
  FROM {{ ref(create_stg_audience_channel_by_day_cross_join(
      person_and_transaction="stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction"
    )) }}
) AS combined_data
-- Check for significant difference between total rows and unique audience/channel counts
HAVING
  COUNT(*) > 10 * (COUNT(DISTINCT donor_audience) + COUNT(DISTINCT channel))
