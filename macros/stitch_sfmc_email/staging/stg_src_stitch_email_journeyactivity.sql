{% macro create_stg_src_stitch_email_journeyactivity() %}

    with deduplicated_data as (
        select distinct
            __versionid_ as version_id,
            activityid as activity_id,
            activityname as activity_name,
            activityexternalkey as activity_external_key,
            journeyactivityobjectid as journey_activity_object_id,
            activitytype as activity_type,
            row_number() over (partition by activityid order by __versionid_) as row_num
        from {{ source("stitch_sfmc_email", "journeyactivity") }}
        where activityid is not null
    )

    select distinct
        version_id,
        activity_id,
        activity_name,
        activity_external_key,
        journey_activity_object_id,
        activity_type
    from deduplicated_data
    where row_num = 1

{% endmacro %}
