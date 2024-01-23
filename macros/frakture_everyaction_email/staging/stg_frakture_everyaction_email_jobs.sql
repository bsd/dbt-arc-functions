{% macro create_stg_frakture_everyaction_email_jobs(
    reference_name="stg_frakture_everyaction_email_summary_unioned"
) %}

select distinct
    safe_cast(message_id as string) as message_id,
    safe_cast(from_name as string) as from_name,
    safe_cast(from_email as string) as from_email,
    safe_cast(
        {{ dbt_date.convert_timezone("cast(publish_date as TIMESTAMP)") }} as timestamp
    ) as best_guess_timestamp,
    safe_cast(null as timestamp) as scheduled_timestamp,
    safe_cast(null as timestamp) as pickup_timestamp,
    safe_cast(null as timestamp) as delivered_timestamp,
    safe_cast(label as string) as email_name,
    safe_cast(subject as string) as email_subject,
    regexp_replace(
        cast(final_primary_source_code as string), '(hdr|ftr)$', ''
    ) as source_code
from {{ ref(reference_name) }}

{% endmacro %}
