{% macro create_stg_stitch_sfmc_audience_transaction_onetime_donor_counts_yearly(
    reference_name="stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction"
) %}
    {{ dbt_arc_functions.util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval('onetime','year', reference_name) }}
{% endmacro %}
