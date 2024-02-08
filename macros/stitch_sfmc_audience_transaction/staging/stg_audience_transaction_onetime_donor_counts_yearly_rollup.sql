{% macro create_stg_audience_transaction_onetime_donor_counts_yearly_rollup(
    person_and_transaction="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    cross_join="stg_audience_channel_by_day_cross_join"
) %}
{{
    dbt_arc_functions.util_stg_audience_transaction_frequency_donor_counts_interval_rollup(
        frequency="onetime", interval="year"
    )
}}
{% endmacro %}
