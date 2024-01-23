{% macro create_stg_src_stitch_email_journey() %}

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
    journeystatus as journey_status
from {{ source("stitch_sfmc_email", "journey") }}
where journeyid is not null

{% endmacro %}
