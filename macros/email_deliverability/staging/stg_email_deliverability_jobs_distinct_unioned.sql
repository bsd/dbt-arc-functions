{% macro create_stg_email_deliverability_jobs_distinct_unioned() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^stg_.*_deliverability_jobs_distinct$"
    ) %}
    {{ dbt_utils.union_relations(relations) }}
{% endmacro %}
