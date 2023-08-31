{% macro create_mart_audience_budget_with_audience_transaction(
    reference_0_name='stg_audience_transaction_onetime_donor_counts_actuals_rollup_unioned',
    reference_1_name='stg_audience_budget_onetime_donor_count_budget_combined') %}
SELECT
    onetime_donor_counts.total_onetime_donor_counts
        AS total_onetime_donor_counts,
    onetime_donor_counts.new_onetime_donor_counts
        AS new_onetime_donor_counts,
    onetime_donor_counts.retained_onetime_donor_counts
        AS retained_onetime_donor_counts,
    onetime_donor_counts.retained3_onetime_donor_counts
        AS retained3_onetime_donor_counts,
    onetime_donor_counts.active_onetime_donor_counts
        AS active_onetime_donor_counts,
    onetime_donor_counts.lapsed_onetime_donor_counts
        AS lapsed_onetime_donor_counts,
    onetime_donor_counts.total_onetime_donor_counts_cumulative
        AS total_onetime_donor_counts_cumulative,
    onetime_donor_counts.new_onetime_donor_counts_cumulative
        AS new_onetime_donor_counts_cumulative,
    onetime_donor_counts.retained_onetime_donor_counts_cumulative
        AS retained_onetime_donor_counts_cumulative,
    onetime_donor_counts.retained3_onetime_donor_counts_cumulative
        AS retained3_onetime_donor_counts_cumulative,
    onetime_donor_counts.reinstated_onetime_donor_counts_cumulative
        AS reinstated_onetime_donor_counts_cumulative,
    onetime_donor_counts.active_onetime_donor_counts_cumulative
        AS active_onetime_donor_counts_cumulative,
    onetime_donor_counts.lapsed_onetime_donor_counts_cumulative
        AS lapsed_onetime_donor_counts_cumulative,
    audience_budget.onetime_donor_count_budget
        AS onetime_donor_count_budget,
    audience_budget.onetime_new_donor_count_budget
        AS onetime_new_donor_count_budget,
    audience_budget.onetime_donor_count_budget_cumulative
        AS onetime_donor_count_budget_cumulative,
    audience_budget.onetime_new_donor_count_cumulative
        AS onetime_new_donor_count_budget_cumulative,
    COALESCE(onetime_donor_counts.date_day, audience_budget.date_day)
        AS date_day,
    COALESCE(
        onetime_donor_counts.interval_type, audience_budget.interval_type
    ) AS interval_type,
    COALESCE(
        onetime_donor_counts.donor_audience, audience_budget.donor_audience
    ) AS donor_audience,
    COALESCE(onetime_donor_counts.platform, audience_budget.join_source)
        AS platform
FROM
    {{ ref(reference_0_name) }}
        AS onetime_donor_counts
FULL JOIN
    {{ ref(reference_1_name) }}
        AS audience_budget
    ON
        onetime_donor_counts.date_day = audience_budget.date_day
        AND UPPER(onetime_donor_counts.interval_type)
        = UPPER(audience_budget.interval_type)
        AND UPPER(onetime_donor_counts.donor_audience)
        = UPPER(audience_budget.donor_audience)
        AND UPPER(onetime_donor_counts.platform)
        = UPPER(audience_budget.join_source)

{% endmacro %}