{% macro create_stg_stitch_sfmc_arc_donor_loyalty_by_fiscal_year(
    audience_union_transaction_joined="stg_stitch_sfmc_arc_audience_union_transaction_joined",
    donor_loyalty_count="stg_stitch_sfmc_arc_donor_loyalty_fiscal_year_count"
) %}


WITH donation_history AS (
    SELECT
        person_id,
        fiscal_year,
        LAG(fiscal_year) OVER (PARTITION BY person_id ORDER BY fiscal_year) AS previous_fiscal_year,
        LAG(fiscal_year, 2) OVER (PARTITION BY person_id ORDER BY fiscal_year) AS fiscal_year_before_previous,
        MAX(transaction_date_day) AS last_donation_date
    FROM
       {{ ref(audience_union_transaction_joined) }}
    GROUP BY
        person_id,
        fiscal_year
)
SELECT
    donor_loyalty_counts.person_id,
    donor_loyalty_counts.fiscal_year,
    donor_loyalty_counts.start_date,
    donor_loyalty_counts.end_date,
    CASE
        WHEN donation_history.previous_fiscal_year IS NULL THEN 'new_donor'
        WHEN donation_history.previous_fiscal_year = donor_loyalty_counts.fiscal_year - 1 AND donation_history.fiscal_year_before_previous IS NULL THEN 'retained_donor'
        WHEN donation_history.previous_fiscal_year = donor_loyalty_counts.fiscal_year - 1 AND donation_history.fiscal_year_before_previous IS NOT NULL THEN 'retained_3+_donor'
        WHEN donation_history.previous_fiscal_year <> donor_loyalty_counts.fiscal_year - 1 THEN 'reactivated_donor'
    END AS donor_loyalty
FROM
    {{ ref(donor_loyalty_count) }} donor_loyalty_counts
LEFT JOIN
    donation_history
ON
    donor_loyalty_counts.person_id = donation_history.person_id
    AND donor_loyalty_counts.fiscal_year = donation_history.fiscal_year
ORDER BY
    donor_loyalty_counts.person_id,
    donor_loyalty_counts.fiscal_year

{% endmacro %}


