{% macro create_stg_stitch_sfmc_transactions_unioned() %}
{% set relations = dbt_arc_functions.relations_that_match_regex(
    "^stg_stitch_.*_transaction$"
) %}
with base as ({{ dbt_utils.union_relations(relations) }})

select 
date_trunc(base.transaction_date, 'day') as transaction_date_day,
base.*
from base

{% endmacro %}
