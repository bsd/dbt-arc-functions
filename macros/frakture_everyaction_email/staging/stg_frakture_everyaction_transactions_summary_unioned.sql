{% macro create_stg_frakture_everyaction_transactions_summary_unioned() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^everyaction_[A-Za-z0-9]{3}_transaction$",
        is_source=True,
        source_name="frakture_everyaction_email",
        schema_to_search="src_frakture",
    ) %}
    select distinct *
    from ({{ dbt_utils.union_relations(relations) }})
{% endmacro %}
