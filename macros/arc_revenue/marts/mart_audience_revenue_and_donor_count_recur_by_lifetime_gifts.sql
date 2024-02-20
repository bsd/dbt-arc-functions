{% macro create_mart_audience_revenue_and_donor_count_recur_by_lifetime_gifts() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_.*_arc_revenue_and_donor_count_by_lifetime_gifts$"
    ) %}

    /* this mart captures only recurring donors! */
    {{ dbt_utils.union_relations(relations) }}
{% endmacro %}
