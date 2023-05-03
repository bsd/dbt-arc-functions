{% macro create_stg_stitch_sfmc_email_jobs() %}
{% set relations = dbt_arc_functions.relations_that_match_regex(
    "^job$",
    is_source=True,
    source_name="stitch_sfmc_email",
    schema_to_search="src_stitch_sfmc_authorized",
) %}

select distinct
    cast(__jobid_ as string) as message_id,
    safe_cast(emailid as string) as email_id,
    safe_cast(fromname as string) as from_name,
    safe_cast(fromemail as string) as from_email,
    case
        when coalesce(schedtime, pickuptime, null) = ''
        then safe_cast(null as timestamp)
        else
            safe_cast(
                {{
                    dbt_date.convert_timezone(
                        "cast(coalesce(schedtime, pickuptime, NULL) as timestamp)"
                    )
                }} as timestamp
            )
    end as best_guess_timestamp,
    case
        when schedtime = ''
        then safe_cast(null as timestamp)
        else
            safe_cast(
                {{
                    dbt_date.convert_timezone(
                        "cast(coalesce(schedtime, NULL) as timestamp)"
                    )
                }} as timestamp
            )
    end as scheduled_timestamp,
    case
        when pickuptime = ''
        then safe_cast(null as timestamp)
        else
            safe_cast(
                {{
                    dbt_date.convert_timezone(
                        "cast(coalesce(pickuptime, NULL) as timestamp)"
                    )
                }} as timestamp
            )
    end as pickup_timestamp,
    case
        when deliveredtime = ''
        then safe_cast(null as timestamp)
        else
            safe_cast(
                {{
                    dbt_date.convert_timezone(
                        "cast(coalesce(deliveredtime, NULL) as timestamp)"
                    )
                }} as timestamp
            )
    end as delivered_timestamp,
    safe_cast(emailname as string) as email_name,
    safe_cast(emailsubject as string) as email_subject,
    safe_cast(category as string) as category,
    safe_cast(null as string) as source_code
from ({{ dbt_utils.union_relations(relations) }})
{% endmacro %}
