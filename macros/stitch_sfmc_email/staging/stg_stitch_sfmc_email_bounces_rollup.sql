{% macro create_stg_stitch_sfmc_email_bounces_rollup(
    reference_name="stg_src_stitch_email_bounce"
) %}
select
    safe_cast(job_id as string) as message_id,
    count(
        distinct case when bounce_category_id = '1' then subscriber_key else null end
    ) as hard_bounces,
    count(
        distinct case when bounce_category_id = '2' then subscriber_key else null end
    ) as soft_bounces,
    count(
        distinct case when bounce_category_id = '3' then subscriber_key else null end
    ) as block_bounces,
    count(
        distinct case when bounce_category_id = '5' then subscriber_key else null end
    ) as tech_bounces,
    count(
        distinct case when bounce_category_id = '4' then subscriber_key else null end
    ) as unknown_bounces,
    count(distinct subscriber_key) as total_bounces
from {{ ref(reference_name) }}
group by 1
{% endmacro %}
