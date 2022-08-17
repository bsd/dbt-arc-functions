{% macro create_mart_transactions_daily_rollup(
    reference_name='stg_transactions_unioned') %}
SELECT
  best_guess_message_id,
  EXTRACT(DATE
  FROM
    transaction_timestamp) AS date_timestamp,
  transaction_source_code,
  channel AS channel_from_crm,
  channel_from_source_code,
  COALESCE(channel, channel_from_source_code) AS channel_best_guess,
  campaign,
  audience,
  crm_entity,
  source_code_entity,
  coalesce(crm_entity,source_code_entity) as best_guess_entity
  SUM(amount) AS total_amount,
  SUM(CASE
      WHEN recurring_revenue THEN amount
    ELSE
    0
  END
    ) total_recurring_revenue,
  SUM(CASE
      WHEN new_recurring_revenue THEN amount
    ELSE
    0
  END
    ) total_new_recurring_revenue,
  COUNT(DISTINCT person_id) AS number_of_donors,
  COUNT(transaction_id_in_source_crm) AS number_of_transactions
FROM
  {{ ref(reference_name) }}
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
    10,
    11
{% endmacro %}