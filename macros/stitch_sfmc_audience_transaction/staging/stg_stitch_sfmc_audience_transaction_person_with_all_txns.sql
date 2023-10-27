{% macro create_stg_stitch_sfmc_audience_transaction_person_with_all_txns(
    stg_stitch_sfmc_audience_transactions_summary_unioned="stg_stitch_sfmc_audience_transactions_summary_unioned",
    max_date=""
) %}

WITH
  start_of_active_and_lapsed AS (
  SELECT
    person_id,
    transaction_date,
    CASE
      WHEN LAG(transaction_date)OVER(PARTITION BY person_id ORDER BY transaction_date) IS NULL THEN transaction_date
      WHEN DATE_DIFF(transaction_date, LAG(transaction_date)OVER(PARTITION BY person_id ORDER BY transaction_date), day) > 426 THEN transaction_date
  END
    AS start_of_active,
    CASE
      WHEN DATE_DIFF(LEAD(transaction_date)OVER(PARTITION BY person_id ORDER BY transaction_date), transaction_date, day) > 426 THEN DATE_ADD(transaction_date, INTERVAL 426 day)
      WHEN LEAD(transaction_date)OVER(PARTITION BY person_id ORDER BY transaction_date) IS NULL
    AND DATE_DIFF(current_date, transaction_date, day) > 426 THEN DATE_ADD(transaction_date, INTERVAL 426 day)
  END
    AS start_of_lapsed
  FROM (
    SELECT
      DISTINCT person_id,
      DATE(transaction_date) AS transaction_date
    FROM
      {{ref('stg_stitch_sfmc_audience_transactions_summary_unioned')}} ) AS person_with_all_transaction_dates

  ORDER BY
    1,
    2),
    donor_engagement_start_dates AS (
  SELECT
    person_id,
    'active' AS donor_engagement,
    start_of_active AS start_date,
  FROM
    start_of_active_and_lapsed
  WHERE
    start_of_active IS NOT NULL
  UNION ALL
  SELECT
    person_id,
    'lapsed' AS donor_engagement,
    start_of_lapsed AS start_date,
  FROM
    start_of_active_and_lapsed
  WHERE
    start_of_lapsed IS NOT NULL
  ORDER BY
    1,
    3)
  SELECT
    person_id,
    donor_engagement,
    start_date,
    LEAD(start_date)OVER(PARTITION BY person_id ORDER BY start_date) - 1 AS end_date
  FROM
    donor_engagement_start_dates
  ORDER BY
    1,
    3 

{% endmacro %}
