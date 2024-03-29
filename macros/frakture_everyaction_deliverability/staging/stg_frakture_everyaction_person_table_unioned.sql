{% macro create_stg_frakture_everyaction_person_table_unioned() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^everyaction_[A-Za-z0-9]{3}_person$",
        is_source=True,
        source_name="frakture_everyaction_deliverability",
        schema_to_search="src_frakture",
    ) %}
    {{ dbt_utils.union_relations(relations) }}
{% endmacro %}
