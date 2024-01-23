{% macro create_stg_stitch_sfmc_deliverability_jobs_distinct() %}
with
    unions as (
        select message_id, sent_date, email_domain
        from {{ ref("stg_stitch_sfmc_deliverability_actions_daily_rollup") }}
        union
        distinct
        select message_id, sent_date, email_domain
        from {{ ref("stg_stitch_sfmc_deliverability_bounces_daily_rollup") }}
        union
        distinct
        select message_id, sent_date, email_domain
        from {{ ref("stg_stitch_sfmc_deliverability_clicks_daily_rollup") }}
        union
        distinct
        select message_id, sent_date, email_domain
        from {{ ref("stg_stitch_sfmc_deliverability_opens_daily_rollup") }}
        union
        distinct
        select message_id, sent_date, email_domain
        from {{ ref("stg_stitch_sfmc_deliverability_unsubscribes_daily_rollup") }}
    )

select distinct *
from unions
{% endmacro %}
