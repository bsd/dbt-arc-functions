{% macro create_stg_audience_transaction_onetime_donor_counts_yearly() %}
    {{
        dbt_arc_functions.util_stg_audience_transaction_frequency_donor_counts_interval(
            "onetime", "year"
        )
    }}
{% endmacro %}