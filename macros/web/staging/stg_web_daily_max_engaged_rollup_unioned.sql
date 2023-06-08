{% macro create_stg_web_daily_max_engaged_rollup_unioned() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_.*_web_daily_max_engaged_users_rollup$"
    ) %}
    {{ dbt_utils.union_relations(relations) }}
{% endmacro %}
