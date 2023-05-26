{% macro create_stg_stitch_sfmc_email_campaign_dates(
    reference_name="stg_src_stitch_email_job"
) %}

    {% if var.database == "bsd-arc-uusa" %}

select
    safe_cast(coalesce(sched_dt, pickup_dt) as timestamp) as campaign_timestamp,
    {{ create_uusa_campaigns_sql() }}
from {{ ref(reference_name) }}

    {% else %}

-- try UUSA as well here and see what happens
select
    safe_cast(coalesce(sched_dt, pickup_dt) as timestamp) as campaign_timestamp,
    {{ create_uusa_campaigns_sql() }}
from {{ ref(reference_name) }}

    {% endif %}

{% endmacro %}
