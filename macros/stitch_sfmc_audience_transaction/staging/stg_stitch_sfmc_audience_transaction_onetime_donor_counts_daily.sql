{% macro create_stg_stitch_sfmc_audience_transaction_onetime_donor_counts_daily(
    reference_name="stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction"
) %}
    {{ util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval('onetime','day', reference_name) }}
{% endmacro %}
