-- depends_on: {{ ref('stg_frakture_everyaction_email_jobs') }}
{% set relations = relations_that_match_regex('^stg_.*_email_jobs$') %}
{{ dbt_utils.union_relations(relations) }}