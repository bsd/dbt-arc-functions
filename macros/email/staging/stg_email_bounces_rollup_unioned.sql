-- depends_on: {{ ref('stg_frakture_everyaction_email_bounces_rollup') }}
{% set relations = relations_that_match_regex('^stg_.*_email_bounces_rollup$') %}
{{ dbt_utils.union_relations(relations) }}