{% macro create_stg_paidmedia_pacing_budget_with_datespine() %}

with
    base as (

        select
            campaign_start_date,
            campaign_end_date,
            generate_date_array(campaign_start_date, campaign_end_date) as date_day,
            campaign_name,
            safe_divide(
                budget, (date_diff(campaign_end_date, campaign_start_date, day) + 1)
            ) as daily_budget,
            descriptions
        from {{ ref("stg_adhoc_google_spreadsheets_paidmedia_pacing_budget") }}

    )

select date_day, campaign_name, daily_budget
from base
-- cross join with unnested date_day array to create a row for each day
cross join unnest(date_day) as date_day
-- filter out dates that are outside of the campaign start and end dates
where date_day >= campaign_start_date and date_day <= campaign_end_date

{% endmacro %}
