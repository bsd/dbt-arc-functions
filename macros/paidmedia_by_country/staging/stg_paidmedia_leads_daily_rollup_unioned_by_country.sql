{% macro create_stg_paidmedia_leads_daily_rollup_unioned_by_country() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_.*_paidmedia_leads_daily_rollup_by_country$"
    ) %}
    {{ dbt_utils.union_relations(relations) }}
{% endmacro %}
