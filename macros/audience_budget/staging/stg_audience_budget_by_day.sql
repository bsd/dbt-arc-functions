{% macro create_stg_audience_budget_by_day(
    google_spreadsheets_audience_monthly_budget="stg_adhoc_google_spreadsheets_audience_monthly_budget"
) %}
    

    with date_spine as (
        {% set min_date_query %}
        SELECT min(start_date) FROM {{ ref(google_spreadsheets_audience_monthly_budget) }}
        {% endset %}
        {% set min_date_results = run_query(min_date_query) %}
        {% if execute %}
            {% set min_date %}'{{min_date_results.columns[0].values()[0]}}'{% endset %}
        {% else %} {% set min_date = "2020-01-01" %}
        {% endif %}

        {% set max_date_query %}
        SELECT max(end_date) FROM {{ ref(google_spreadsheets_audience_monthly_budget) }}
        {% endset %}
        {% set max_date_results = run_query(max_date_query) %}
        {% if execute %}
            {% set max_date %}'{{max_date_results.columns[0].values()[0]}}'{% endset %}
        {% else %} {% set max_date = "2020-01-01" %}
        {% endif %}

        {{ dbt_utils.date_spine(datepart="day", start_date=min_date, end_date=max_date) }}


    )

    ,
        dailies as (
        {% set number_of_days_in_budget = ("date_diff(budget.end_date, budget.start_date, day)") %}
            select
                platform,
                donor_audience,
                date_spine.date_day,
                safe_cast(
                    extract(year from date_spine.date_day) as int
                ) as date_spine_year,
                safe_cast(
                    extract(month from date_spine.date_day) as int
                ) as date_spine_month,
                safe_cast(
                    extract(day from date_spine.date_day) as int
                ) as date_spine_day,
                total_revenue_budget
                / {{ number_of_days_in_budget }} as total_revenue_budget_by_day,
                loyalty_new_donor_targets
                / {{ number_of_days_in_budget }} as loyalty_new_donor_targets_by_day,
                loyalty_unknown_donor_targets
                / {{ number_of_days_in_budget }}
                as loyalty_unknown_donor_targets_by_day,
                loyalty_retained_donor_targets
                / {{ number_of_days_in_budget }}
                as loyalty_retained_donor_targets_by_day,
                loyalty_retained_three_donor_targets
                / {{ number_of_days_in_budget }}
                as loyalty_retained_three_donor_targets_by_day,
                loyalty_reinstated_donor_targets
                / {{ number_of_days_in_budget }}
                as loyalty_reinstated_donor_targets_by_day

            from {{ ref(google_spreadsheets_audience_monthly_budget) }} as budget
            inner join
                date_spine
                on budget.start_date <= date_spine.date_day
                and budget.end_date >= date_spine.date_day
            order by 1, 2, 3
        )

    select
        platform,
        donor_audience,
        date_day,
        date_spine_year,
        date_spine_month,
        date_spine_day,
        total_revenue_budget_by_day,
        loyalty_new_donor_targets_by_day,
        loyalty_unknown_donor_targets_by_day,
        loyalty_retained_donor_targets_by_day,
        loyalty_retained_three_donor_targets_by_day,
        loyalty_reinstated_donor_targets_by_day,
        loyalty_new_donor_targets_by_day
        + loyalty_unknown_donor_targets_by_day
        + loyalty_retained_donor_targets_by_day
        + loyalty_retained_three_donor_targets_by_day
        + loyalty_reinstated_donor_targets_by_day as total_donors_by_day
    from dailies

{% endmacro %}
