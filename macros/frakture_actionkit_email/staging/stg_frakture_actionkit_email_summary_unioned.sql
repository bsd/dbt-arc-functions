{% macro create_stg_frakture_actionkit_email_summary_unioned() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^actionkit_[A-Za-z0-9]{3}_email_summary_by_date$",
        is_source=True,
        source_name="frakture_actionkit_email",
        schema_to_search="src_frakture",
    ) %}
    select distinct *
    from ({{ dbt_utils.union_relations(relations) }})
    where message_id is not null
{% endmacro %}
