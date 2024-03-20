-- fmt: off
{% macro create_stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched() %}

{{dbt_arc_functions.util_stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched(
    is_digital='False'
)}}

{% endmacro %}
