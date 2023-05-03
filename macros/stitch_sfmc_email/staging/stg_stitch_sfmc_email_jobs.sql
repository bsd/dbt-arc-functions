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
    safe_cast(
        {{ dbt_date.convert_timezone("cast(coalesce(schedtime, pickuptime) as timestamp)") }} as timestamp
    ) as best_guess_timestamp,
    safe_cast( {{ dbt_date.convert_timezone("cast(schedtime as timestamp)") }} as timestamp) as scheduled_timestamp,
    safe_cast({{ dbt_date.convert_timezone("cast(pickuptime as timestamp)") }} as timestamp) as pickup_timestamp,
    safe_cast({{ dbt_date.convert_timezone("cast(deliveredtime as timestamp)") }} as timestamp) as delivered_timestamp,
    safe_cast(emailname as string) as email_name,
    safe_cast(emailsubject as string) as email_subject,
    safe_cast(category as string) as category,
    safe_cast(null as string) as source_code
from ({{ dbt_utils.union_relations(relations) }})
{% endmacro %}
