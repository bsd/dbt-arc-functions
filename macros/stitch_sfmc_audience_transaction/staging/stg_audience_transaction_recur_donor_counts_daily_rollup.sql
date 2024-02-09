{% macro create_stg_audience_transaction_recur_donor_counts_daily_rollup(
    person_and_transaction="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched"
) %}
{{
    dbt_arc_functions.util_stg_audience_transaction_frequency_donor_counts_interval_rollup(
        frequency="recurring", interval="day"
    )
}}
{% endmacro %}
