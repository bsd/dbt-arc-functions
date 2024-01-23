{% macro create_stg_audience_recur_donor_counts_by_gift_size_daily(
    audience_transaction="stg_audience_transactions_and_audience_summary"
) %}

    {{
        dbt_arc_functions.util_stg_audience_frequency_donor_counts_by_gift_size_interval(
            frequency="recurring", interval="day"
        )
    }}

{% endmacro %}
