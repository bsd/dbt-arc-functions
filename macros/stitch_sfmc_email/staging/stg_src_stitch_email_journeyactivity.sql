{% macro create_stg_src_stitch_email_journeyactivity() %}
{% set relations= dbt_arc_functions.relations_that_match_regex('^journeyactivity$')}
    is_source=True,
  source_name='stitch_sfmc_email',
  schema_to_search='src_stitch_sfmc_authorized')
%}

      select distinct
        __versionid_ as version_id
        ,activityid as activity_id
        ,activityname as activity_name
        ,activityexternalkey as activity_external_key
        ,journeyactivityobjectid as journey_activity_object_id
        ,activitytype as activity_type
         from {{ref(relations)}}

{% endmacro %}