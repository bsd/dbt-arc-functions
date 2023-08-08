SELECT date_day,
'daily' AS interval_type,
donor_audience,
join_source as platform,
COUNT(person_id) AS total_recur_donor_counts,
COUNT(CASE WHEN donor_loyalty = 'new' THEN 1 END) AS new_recur_donor_counts,
COUNT(CASE WHEN donor_loyalty = 'retained' THEN 1 END) AS retained_recur_donor_counts,
COUNT(CASE WHEN donor_loyalty = 'retained3' THEN 1 END) AS retained3_recur_donor_counts,
COUNT(CASE WHEN donor_engagement = 'active' THEN 1 END) AS active_recur_donor_counts,
COUNT(CASE WHEN donor_engagement = 'lapsed' THEN 1 END) AS lapsed_recur_donor_counts,

FROM ( SELECT
date_day,
person_id,
donor_audience,
donor_engagement,
donor_loyalty,
gift_size_str,
join_source,
join_amount_str,
join_month_year_str,
FROM {{ ref('ARC-1511')}}
)

WHERE donor_audience = 'recurring'
GROUP BY 1, 2, 3