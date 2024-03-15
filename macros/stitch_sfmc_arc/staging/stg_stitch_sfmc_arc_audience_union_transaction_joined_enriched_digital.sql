-- fmt: off
{% macro create_stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched_digital() %}

{{dbt_arc_functions.util_stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched(
    is_digital='True'
)}}

{% endmacro %}
