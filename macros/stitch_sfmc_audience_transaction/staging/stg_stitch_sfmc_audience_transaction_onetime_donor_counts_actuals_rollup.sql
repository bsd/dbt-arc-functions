{% macro create_stg_stitch_sfmc_audience_transaction_onetime_donor_counts_actuals_rollup(
    reference_name='stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction') %}
WITH daily_rollup AS (
    SELECT
        date_day AS date_day,
        'daily' AS interval_type,
        donor_audience AS donor_audience,
        join_source AS platform,
        count(DISTINCT person_id) AS total_onetime_donor_counts,
        count(DISTINCT CASE WHEN donor_loyalty = 'new_donor' THEN person_id END)
            AS new_onetime_donor_counts,
        count(DISTINCT CASE WHEN donor_loyalty = 'retained' THEN person_id END)
            AS retained_onetime_donor_counts,
        count(DISTINCT CASE WHEN donor_loyalty = 'retained3' THEN person_id END)
            AS retained3_onetime_donor_counts,
        count(
            DISTINCT CASE
                WHEN donor_engagement = 'reinstated' THEN person_id
            END
        )
            AS reinstated_onetime_donor_counts,
        count(DISTINCT CASE WHEN donor_engagement = 'active' THEN person_id END)
            AS active_onetime_donor_counts,
        count(DISTINCT CASE WHEN donor_engagement = 'lapsed' THEN person_id END)
            AS lapsed_onetime_donor_counts,
        {{ dbt_arc_functions.get_fiscal_year('date_day', var('fiscal_year_start')) }} AS fiscal_year,
    FROM
        {{ ref(reference_name) }}
    WHERE donor_audience != 'recurring'
    GROUP BY 1, 2, 3, 4
)

SELECT
    date_day AS date_day,
    interval_type AS interval_type,
    donor_audience AS donor_audience,
    platform AS platform,
    total_onetime_donor_counts AS total_onetime_donor_counts,
    new_onetime_donor_counts AS new_onetime_donor_counts,
    retained_onetime_donor_counts AS retained_onetime_donor_counts,
    retained3_onetime_donor_counts AS retained3_onetime_donor_counts,
    active_onetime_donor_counts AS active_onetime_donor_counts,
    lapsed_onetime_donor_counts AS lapsed_onetime_donor_counts,
    sum(total_onetime_donor_counts)
        OVER (PARTITION BY fiscal_year, interval_type, donor_audience, platform ORDER BY date_day)
        AS total_onetime_donor_counts_cumulative,
    sum(new_onetime_donor_counts)
        OVER (PARTITION BY fiscal_year, interval_type, donor_audience, platform ORDER BY date_day)
        AS new_onetime_donor_counts_cumulative,
    sum(retained_onetime_donor_counts)
        OVER (PARTITION BY fiscal_year, interval_type, donor_audience, platform ORDER BY date_day)
        AS retained_onetime_donor_counts_cumulative,
    sum(retained3_onetime_donor_counts)
        OVER (PARTITION BY fiscal_year, interval_type, donor_audience, platform ORDER BY date_day)
        AS retained3_onetime_donor_counts_cumulative,
    sum(reinstated_onetime_donor_counts)
        OVER (PARTITION BY fiscal_year, interval_type, donor_audience, platform ORDER BY date_day)
        AS reinstated_onetime_donor_counts_cumulative,
    sum(active_onetime_donor_counts)
        OVER (PARTITION BY fiscal_year, interval_type, donor_audience, platform ORDER BY date_day)
        AS active_onetime_donor_counts_cumulative,
    sum(lapsed_onetime_donor_counts)
        OVER (PARTITION BY fiscal_year, interval_type, donor_audience, platform ORDER BY date_day)
        AS lapsed_onetime_donor_counts_cumulative
FROM daily_rollup
ORDER BY 1, 2, 3, 4
{% endmacro %}