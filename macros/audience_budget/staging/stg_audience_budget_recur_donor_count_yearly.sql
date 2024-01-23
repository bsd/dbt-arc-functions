{% macro create_stg_audience_budget_recur_donor_count_yearly() %}

{{
    dbt_arc_functions.util_stg_audience_budget_frequency_donor_counts_interval_type(
        interval="year", frequency="recurring"
    )
}}

{% endmacro %}
