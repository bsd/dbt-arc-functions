{% macro create_stg_stitch_sfmc_donor_audience_calculated_scd(
    calculated_audience_with_date_spine ="stg_stitch_sfmc_donor_audience_calculated_with_date_spine",
    calculated_date_spine = "stg_stitch_sfmc_audience_transaction_calculated_date_spine"
) %}


WITH changes AS (
  SELECT
    person_id,
    transaction_date_day,
    donor_audience,
    LAG(donor_audience) OVER (PARTITION BY person_id ORDER BY transaction_date_day) AS prev_donor_audience
  FROM {{ ref(calculated_audience_with_date_spine) }}
)
SELECT
  person_id,
  MIN(transaction_date_day) AS start_date,
  IFNULL(MAX(next_date) -  1, (SELECT MAX(date) FROM {{ ref(calculated_date_spine) }})) AS end_date,
  donor_audience
FROM (
  SELECT
    person_id,
    transaction_date_day,
    donor_audience,
    LEAD(transaction_date_day) OVER (PARTITION BY person_id ORDER BY transaction_date_day) AS next_date
  FROM changes
  WHERE prev_donor_audience IS NULL OR donor_audience != prev_donor_audience
) filtered_changes
GROUP BY person_id, donor_audience, next_date
ORDER BY person_id, start_date

{% endmacro %}