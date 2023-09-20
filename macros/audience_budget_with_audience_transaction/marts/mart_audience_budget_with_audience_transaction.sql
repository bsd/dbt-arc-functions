{% macro create_mart_audience_budget_with_audience_transaction(
    reference_0_name='stg_audience_transaction_onetime_donor_counts_actuals_rollup_unioned',
    reference_1_name='stg_audience_budget_onetime_donor_count_budget_combined') %}
    select
        onetime_donor_counts.total_onetime_donor_counts as total_onetime_donor_counts,
        onetime_donor_counts.new_onetime_donor_counts as new_onetime_donor_counts,
        onetime_donor_counts.retained_onetime_donor_counts
        as retained_onetime_donor_counts,
        onetime_donor_counts.retained3_onetime_donor_counts
        as retained3_onetime_donor_counts,
        onetime_donor_counts.active_onetime_donor_counts as active_onetime_donor_counts,
        onetime_donor_counts.lapsed_onetime_donor_counts as lapsed_onetime_donor_counts,
        onetime_donor_counts.total_onetime_donor_counts_cumulative
        as total_onetime_donor_counts_cumulative,
        onetime_donor_counts.new_onetime_donor_counts_cumulative
        as new_onetime_donor_counts_cumulative,
        onetime_donor_counts.retained_onetime_donor_counts_cumulative
        as retained_onetime_donor_counts_cumulative,
        onetime_donor_counts.retained3_onetime_donor_counts_cumulative
        as retained3_onetime_donor_counts_cumulative,
        onetime_donor_counts.reinstated_onetime_donor_counts_cumulative
        as reinstated_onetime_donor_counts_cumulative,
        onetime_donor_counts.active_onetime_donor_counts_cumulative
        as active_onetime_donor_counts_cumulative,
        onetime_donor_counts.lapsed_onetime_donor_counts_cumulative
        as lapsed_onetime_donor_counts_cumulative,
        audience_budget.onetime_donor_count_budget as onetime_donor_count_budget,
        audience_budget.onetime_new_donor_count_budget
        as onetime_new_donor_count_budget,
        audience_budget.onetime_donor_count_budget_cumulative
        as onetime_donor_count_budget_cumulative,
        audience_budget.onetime_new_donor_count_cumulative
        as onetime_new_donor_count_budget_cumulative,
        coalesce(onetime_donor_counts.date_day, audience_budget.date_day) as date_day,
        coalesce(
            onetime_donor_counts.interval_type, audience_budget.interval_type
        ) as interval_type,
        coalesce(
            onetime_donor_counts.donor_audience, audience_budget.donor_audience
        ) as donor_audience,
        coalesce(onetime_donor_counts.platform, audience_budget.join_source) as platform
    from {{ ref(reference_0_name) }} as onetime_donor_counts
    full join
        {{ ref(reference_1_name) }} as audience_budget
        on onetime_donor_counts.date_day = audience_budget.date_day
        and upper(onetime_donor_counts.interval_type)
        = upper(audience_budget.interval_type)
        and upper(onetime_donor_counts.donor_audience)
        = upper(audience_budget.donor_audience)
        and upper(onetime_donor_counts.platform) = upper(audience_budget.join_source)

{% endmacro %}
