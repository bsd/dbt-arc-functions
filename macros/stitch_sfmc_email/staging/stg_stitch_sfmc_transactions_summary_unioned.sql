{% macro create_stg_stitch_sfmc_transactions_summary_unioned() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_stitch_sfmc_[A-Za-z0-9]{3}_transactions$",
    ) %}
    select distinct *
    from ({{ dbt_utils.union_relations(relations) }})
    where transaction_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
{% endmacro %}
