{% macro create_stg_stitch_sfmc_email_unsubscribes_rollup(
    reference_name="stg_src_stitch_email_unsubscribe"
) %}
select
    safe_cast(job_id as string) as message_id,
    safe_cast(count(distinct subscriber_key) as int) as unsubscribes
from {{ ref(reference_name) }}
group by 1
{% endmacro %}
