-- depends_on: {{ ref('stg_stitch_sfmc_fundraiseup_filtered_recent_transaction') }}
-- depends_on: {{ ref('stg_stitch_sfmc_bbcrm_transaction') }}
{% macro create_stg_stitch_transactions_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex('^stg_stitch_.*_transaction$') %}
{{ dbt_utils.union_relations(relations) }}
{% endmacro %}