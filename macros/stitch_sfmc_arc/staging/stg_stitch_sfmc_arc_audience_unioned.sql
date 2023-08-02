{% macro create_stg_stitch_sfmc_arc_audience_unioned() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_.*_audience_by_date_day$"
    ) %}
    {{ dbt_utils.union_relations(relations) }}
{% endmacro %}


