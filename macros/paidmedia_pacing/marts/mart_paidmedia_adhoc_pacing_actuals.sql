{% macro create_mart_paidmedia_adhoc_pacing_actuals(
    reference_name='mart_paidmedia_adhoc_pacing_actuals_rollup') %}

SELECT DISTINCT * FROM {{ref('reference_name')}}


{% endmacro %}