{% macro create_stg_stitch_sfmc_email_bounces_rollup(
    reference_name="stg_src_stitch_email_bounce"
) %}
    select
        safe_cast(job_id as string) as message_id,
        sum(case when bounce_category_id = '1' then 1 else 0 end) as hard_bounces,
        sum(case when bounce_category_id = '2' then 1 else 0 end) as soft_bounces,
        sum(case when bounce_category_id = '3' then 1 else 0 end) as block_bounces,
        sum(case when bounce_category_id = '5' then 1 else 0 end) as tech_bounces,
        sum(case when bounce_category_id = '4' then 1 else 0 end) as unknown_bounces,
        sum(
            case
                when bounce_category_id = '1'
                then 1
                when bounce_category_id = '2'
                then 1
            end
        ) as total_bounces
    from {{ ref(reference_name) }}
    group by 1
{% endmacro %}
