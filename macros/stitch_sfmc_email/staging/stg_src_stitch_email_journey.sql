{% macro create_stg_src_stitch_email_journey() %}

    with deduplicated_data as (
        select distinct
            __versionid_ as version_id,
            journeyid as journey_id,
            journeyname as journey_name,
            cast(versionnumber as int64) as version_number,
            (
                case
                    when length(createddate) > 0
                    then
                        datetime(
                            cast(
                                concat(
                                    substr(createddate, 0, 22), " America/New_York"
                                ) as timestamp
                            ),
                            "America/New_York"
                        )
                    else null
                end
            ) as created_dt,
            (
                case
                    when length(lastpublisheddate) > 0
                    then
                        datetime(
                            cast(
                                concat(
                                    substr(lastpublisheddate, 0, 22), " America/New_York"
                                ) as timestamp
                            ),
                            "America/New_York"
                        )
                    else null
                end
            ) as last_published_dt,
            (
                case
                    when length(modifieddate) > 0
                    then
                        datetime(
                            cast(
                                concat(
                                    substr(modifieddate, 0, 22), " America/New_York"
                                ) as timestamp
                            ),
                            "America/New_York"
                        )
                    else null
                end
            ) as modified_dt,
            journeystatus as journey_status,
            row_number() over (partition by journeyid order by created_dt) as row_num
        from {{ source("stitch_sfmc_email", "journey") }}
        where journeyid is not null
    )

    select distinct
        version_id,
        journey_id,
        journey_name,
        version_number,
        created_dt,
        last_published_dt,
        modified_dt,
        journey_status
    from deduplicated_data
    where row_num = 1

{% endmacro %}
