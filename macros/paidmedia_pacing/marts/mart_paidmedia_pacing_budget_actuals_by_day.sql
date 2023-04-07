{% macro create_mart_paidmedia_pacing_budget_actuals_by_day(
    jobs="stg_paidmedia_actuals_campaign_jobs",
    daily="stg_paidmedia_actuals_budget_by_day_and_campaign_join"
) %}

with
    base as (
        select
            daily.date_day,
            daily.campaign_start_date,
            daily.campaign_end_date,
            daily.campaign_name,
            daily.daily_spend,
            daily.cumulative_spend,
            daily.spend_pace,
            daily.daily_budget,
            daily.cumulative_budget,
            daily.remaining_budget,
            daily.total_budget,
            daily.budget_pace,
            daily.descriptions,
            jobs.channel,
            jobs.channel_category,
            jobs.channel_type,
            jobs.objective

        from {{ ref(daily) }} daily
        left join {{ ref(jobs) }} jobs on daily.campaign_name = jobs.campaign_name
        where daily.campaign_name is not null and daily.campaign_start_date is not null
    )

select distinct *
from base

{% endmacro %}
