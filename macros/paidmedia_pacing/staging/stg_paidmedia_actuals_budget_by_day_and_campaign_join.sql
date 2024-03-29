{% macro create_stg_paidmedia_actuals_budget_by_day_and_campaign_join(
    actuals="stg_paidmedia_pacing_actuals_campaign_and_date_rollup",
    budget="stg_paidmedia_pacing_budget_datespine_details_join"
) %}

    select
        coalesce(actuals.date_day, budget.date_day) as date_day,
        coalesce(actuals.campaign_name, budget.campaign_name) as campaign_name,
        actuals.daily_spend,
        actuals.cumulative_spend,
        budget.daily_budget,
        coalesce(budget.total_budget, 0)
        - coalesce(actuals.daily_spend, 0) as remaining_budget,
        coalesce(actuals.cumulative_spend / budget.total_budget, 0) as spend_pace,
        coalesce(budget.cumulative_budget / budget.total_budget, 0) as budget_pace,
        budget.cumulative_budget,
        budget.total_budget,
        budget.campaign_start_date,
        budget.campaign_end_date,
        budget.descriptions

    from {{ ref(actuals) }} actuals
    full outer join
        {{ ref(budget) }} budget
        on actuals.date_day = budget.date_day
        and actuals.campaign_name = budget.campaign_name

{% endmacro %}
