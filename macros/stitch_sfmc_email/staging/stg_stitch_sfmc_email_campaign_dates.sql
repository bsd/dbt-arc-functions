{% macro create_stg_stitch_sfmc_email_campaign_dates(
    reference_name="stg_stitch_sfmc_email_jobs"
) %}

{% if var.database == "bsd-arc-uusa" %}

select
    best_guess_timestamp as campaign_timestamp,
    {{ create_uusa_campaigns_sql() }}
from {{ ref(reference_name) }}

{% else %}

-- place holder with nulls for now
select
    best_guess_timestamp as campaign_timestamp,
    null as crm_campaign,
    null as source_code_campaign
from {{ ref(reference_name) }}

{% endif %}

{% endmacro %}
