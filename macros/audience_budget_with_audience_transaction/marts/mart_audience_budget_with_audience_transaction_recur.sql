{% macro create_mart_audience_budget_with_audience_transaction_recur(
    reference_0_name='stg_audience_transaction_recur_donor_counts_actuals_rollup_unioned',
    reference_1_name='stg_audience_budget_recur_donor_counts_with_interval_combined') %}
select
    coalesce(recur_donor_counts.date_day, audience_budget.date_day) as date_day,
    coalesce(recur_donor_counts.interval_type, audience_budget.interval_type)
        as interval_type,
    coalesce(
        recur_donor_counts.donor_audience, audience_budget.donor_audience
    ) as donor_audience,
    coalesce(recur_donor_counts.platform, audience_budget.join_source)
        as platform,
    recur_donor_counts.total_recur_donor_counts as total_recur_donor_counts,
    recur_donor_counts.new_recur_donor_counts as new_recur_donor_counts,
    recur_donor_counts.retained_recur_donor_counts
        as retained_recur_donor_counts,
    recur_donor_counts.retained3_recur_donor_counts
        as retained3_recur_donor_counts,
    recur_donor_counts.active_recur_donor_counts as active_recur_donor_counts,
    recur_donor_counts.lapsed_recur_donor_counts as lapsed_recur_donor_counts,
    audience_budget.recur_total_donor_count_budget
        as recur_total_donor_count_budget,
from
    {{ ref(reference_0_name) }}
        as recur_donor_counts
full join
    {{ ref(reference_1_name) }}
        as audience_budget
    on
        recur_donor_counts.date_day = audience_budget.date_day
        and recur_donor_counts.interval_type = audience_budget.interval_type
        and recur_donor_counts.donor_audience = audience_budget.donor_audience
        and recur_donor_counts.platform = audience_budget.join_source
{% endmacro %}