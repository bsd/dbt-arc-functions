{% macro create_stg_email_jobs_unioned() %}
{% set relations = relations_that_match_regex('^stg_.*_email_jobs$') %}
{{ dbt_utils.union_relations(relations) }}
{% endmacro %}