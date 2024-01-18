{% macro create_stg_stitch_sfmc_audience_transaction_onetime_donor_counts_daily(
) %}
    {{
        dbt_arc_functions.util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval(
            "onetime", "day"
        )
    }}
{% endmacro %}
