{% macro create_stg_src_stitch_email_journeyactivity() %}

select distinct
    __versionid_ as version_id,
    activityid as activity_id,
    activityname as activity_name,
    activityexternalkey as activity_external_key,
    journeyactivityobjectid as journey_activity_object_id,
    activitytype as activity_type
from {{ source("stitch_sfmc_email", "journeyactivity") }}
where activityid is not null

{% endmacro %}
