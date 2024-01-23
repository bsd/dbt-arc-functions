{% macro create_stg_frakture_everyaction_deliverability_bounces_daily_rollup(
    reference_name="stg_frakture_everyaction_deliverability_message_stat_unioned_with_domain"
) %}
with
    base as (
        select
            safe_cast(sent_ts as date) as sent_date,
            safe_cast(message_id as string) as message_id,
            safe_cast(email_domain as string) as email_domain,
            sum(safe_cast(hard_bounce as int)) as hard_bounces,
            sum(safe_cast(soft_bounce as int)) as soft_bounces
        from {{ ref(reference_name) }}
        group by 1, 2, 3
    )

select
    base.*,
    case
        when base.soft_bounces + base.hard_bounces is null
        then 0
        else base.soft_bounces + base.hard_bounces
    end as total_bounces
from base
{% endmacro %}
