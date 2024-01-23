-- fmt: off
{% macro util_mart_audience_budget_with_audience_transaction(
    recur_status,
    onetime_donor_counts_table="stg_audience_transaction_onetime_donor_counts_actuals_rollup_unioned",
    onetime_audience_budget_table="stg_audience_budget_onetime_donor_counts_with_interval_combined",
    recur_donor_counts_table="stg_audience_transaction_recur_donor_counts_actuals_rollup_unioned",
    recur_audience_budget_table="stg_audience_budget_recur_donor_counts_with_interval_combined"
) %}

    {% if recur_status not in ["recurring", "onetime"] %}
        {{
            exceptions.raise_compiler_error(
                "'recur_status' argument to util_stg_stitch_sfmc_audience_transaction_rev_by_cohort must be 'recurring' or 'onetime', got "
                ~ recur_status
            )
        }}
    {% endif %}

    {% set recur_onetime = "recur" if recur_status == "recurring" else "onetime" %}

   select
        coalesce(donor_counts.date_day, audience_budget.date_day) as date_day,
        coalesce(
            donor_counts.interval_type, audience_budget.interval_type
        ) as interval_type,
        coalesce(
            donor_counts.donor_audience, audience_budget.donor_audience
        ) as donor_audience,
        coalesce(
            initcap(donor_counts.channel), initcap(audience_budget.join_source)
        ) as channel,
        donor_counts.total_{{ recur_onetime }}_donor_counts,
        donor_counts.new_{{ recur_onetime }}_donor_counts,
        donor_counts.retained_{{ recur_onetime }}_donor_counts,
        donor_counts.retained3_{{ recur_onetime }}_donor_counts,
        donor_counts.reinstated_{{ recur_onetime }}_donor_counts,
        donor_counts.active_{{ recur_onetime }}_donor_counts,
        donor_counts.lapsed_{{ recur_onetime }}_donor_counts,
        donor_counts.total_{{ recur_onetime }}_donor_counts_cumulative,
        donor_counts.new_{{ recur_onetime }}_donor_counts_cumulative,
        donor_counts.new_to_fy_{{ recur_onetime }}_donor_counts_cumulative,
        donor_counts.retained_{{ recur_onetime }}_donor_counts_cumulative,
        donor_counts.retained3_{{ recur_onetime }}_donor_counts_cumulative,
        donor_counts.reinstated_{{ recur_onetime }}_donor_counts_cumulative,
        audience_budget.{{ recur_onetime }}_donor_count_budget, 
        audience_budget.{{ recur_onetime }}_new_donor_count_budget,
        audience_budget.{{ recur_onetime }}_donor_count_budget_cumulative,
        audience_budget.{{ recur_onetime }}_new_donor_count_cumulative

    {% if recur_status == "onetime" %}
        from {{ ref(onetime_donor_counts_table) }} as donor_counts
        full join
            {{ ref(onetime_audience_budget_table) }} as audience_budget
    {% elif recur_status == "recurring" %}
        from {{ ref(recur_donor_counts_table) }} as donor_counts
        full join
            {{ ref(recur_audience_budget_table) }} as audience_budget
    {% endif %}
        on donor_counts.date_day = audience_budget.date_day
        and upper(donor_counts.interval_type)
        = upper(audience_budget.interval_type)
        and upper(donor_counts.donor_audience)
        = upper(audience_budget.donor_audience)
        and upper(donor_counts.channel) = upper(audience_budget.join_source) 

{% endmacro %}
