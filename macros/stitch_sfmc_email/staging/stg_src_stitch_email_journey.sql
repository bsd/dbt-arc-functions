{% macro create_stg_src_stitch_email_journey() %}
{% set relations= dbt_arc_functions.relations_that_match_regex('^journey$')
    is_source=True,
  source_name='stitch_sfmc_email',
  schema_to_search='src_stitch_sfmc_authorized' %}


Select DISTINCT
        __versionid_ as version_id
        ,journeyid as journey_id
        ,journeyname as journey_name
        ,CAST(versionnumber AS INT64) as version_number
        ,( case
            when length(createddate) > 0 then datetime(CAST(CONCAT(Substr(createddate,0,22)," America/New_York") as timestamp), "America/New_York")
            else null
        end ) as created_dt
        ,( case
            when length(lastpublisheddate) > 0 then datetime(CAST(CONCAT(Substr(lastpublisheddate,0,22)," America/New_York") as timestamp), "America/New_York")
            else null
        end ) as last_published_dt
        ,( case
            when length(modifieddate) > 0 then datetime(CAST(CONCAT(Substr(ModifiedDate,0,22)," America/New_York") as timestamp), "America/New_York")
            else null
        end ) as modified_dt
        ,journeystatus as journey_status


     from {{ref(relations)}}

{% endmacro %}