{% macro create_stg_stitch_sfmc_donor_engagement_scd(
    donor_engagement ='stg_stitch_sfmc_donor_engagement_calculated',
    donor_engagement_date_spine = 'stg_stitch_sfmc_donor_engagement_date_spine'
) %}

WITH changes AS (
  SELECT
    person_id,
    transaction_date_day,
    donor_engagement,
    LAG(donor_engagement) OVER (PARTITION BY person_id ORDER BY transaction_date_day) AS prev_donor_engagement
  FROM {{ ref(donor_engagement) }}
)
SELECT
  person_id,
  MIN(transaction_date_day) AS start_date,
  IFNULL(MAX(next_date) -  1, (SELECT MAX(date) FROM {{ ref(donor_engagement_date_spine) }})) AS end_date,
  donor_engagement
FROM (
  SELECT
    person_id,
    transaction_date_day,
    donor_engagement,
    LEAD(transaction_date_day) OVER (PARTITION BY person_id ORDER BY transaction_date_day) AS next_date
  FROM changes
  WHERE prev_donor_engagement IS NULL OR donor_engagement != prev_donor_engagement
) filtered_changes
GROUP BY person_id, donor_engagement, next_date
ORDER BY person_id, start_date



{% endmacro %}