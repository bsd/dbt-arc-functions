{% macro create_stg_audience_transaction_onetime_donor_counts_daily_rollup(
    person_and_transaction="stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction",
    cross_join="stg_audience_channel_by_day_cross_join"
) %}
{{
    dbt_arc_functions.util_stg_audience_transaction_frequency_donor_counts_interval_rollup(
        frequency="onetime", interval="day"
    )
}}
{% endmacro %}
