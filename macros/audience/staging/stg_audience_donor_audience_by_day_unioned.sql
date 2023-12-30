{% macro create_stg_audience_donor_audience_by_day_unioned() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_\w+_audience_donor_audience_by_day$"
    ) %}
    {{ dbt_utils.union_relations(relations) }}

{% endmacro %}