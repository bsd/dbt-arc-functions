{% macro create_stg_stitch_sfmc_email_campaign_dates(
    reference_name="stg_src_stitch_email_jobs"
) %}

{% if var.database == 'bsd-arc-uusa'%}

select
    safe_cast(coalesce(sched_dt, pickup_dt) as timestamp) as campaign_timestamp,
    {{ create_uusa_campaigns_sql() }}
from {{ ref(reference_name) }}

{% else %}


-- place holder with nulls for now

select
    safe_cast(coalesce(sched_dt, pickup_dt) as timestamp) as campaign_timestamp,
    safe_cast(null as string) as crm_campaign,
    safe_cast(null as string) as source_code_campaign
from {{ ref(reference_name) }}

{% endif %}

{% endmacro %}
