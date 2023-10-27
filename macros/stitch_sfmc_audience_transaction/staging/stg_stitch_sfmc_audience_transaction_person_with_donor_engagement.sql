{% macro create_stg_stitch_sfmc_audience_transaction_person_with_donor_engagement(
    stg_stitch_sfmc_audience_transaction_person_with_all_txns="stg_stitch_sfmc_audience_transaction_person_with_all_txns"
) %}
SELECT
  donor_engagement_start_and_end_dates.person_id,
  date_spine.date_day AS date_day,
  donor_engagement_start_and_end_dates.donor_engagement
FROM
  {{ref('stg_stitch_sfmc_audience_transaction_calculated_date_spine')}} as date_spine
INNER JOIN
  {{ref('stg_stitch_sfmc_audience_transaction_person_with_all_txns')}} as donor_engagement_start_and_end_dates
ON
  date_spine.date_day >= donor_engagement_start_and_end_dates.start_date
  AND date_spine.date_day <= donor_engagement_start_and_end_dates.end_date
  OR donor_engagement_start_and_end_dates.end_date IS NULL
ORDER BY 1, 2
{% endmacro %}
