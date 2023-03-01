{% macro create_stg_src_stitch_email_ftaf() %}
{% set relations= dbt_arc_functions.relations_that_match_regex('^ftaf$')}
    is_source=True,
  source_name='stitch_sfmc_email',
  schema_to_search='src_stitch_sfmc_authorized')



