{% macro create_stg_audience_budget_onetime_donor_count_budget_combined() %}
    select
        date_day,
        interval_type,
        donor_audience,
        join_source,
        onetime_donor_count_budget,
        onetime_new_donor_count_budget,
        onetime_donor_count_budget_cumulative,
        onetime_new_donor_count_cumulative
    from {{ ref("stg_audience_budget_onetime_donor_count_budget_daily") }}
    union all
    select
        date_day,
        interval_type,
        donor_audience,
        join_source,
        onetime_donor_count_budget,
        onetime_new_donor_count_budget,
        onetime_donor_count_budget_cumulative,
        onetime_new_donor_count_cumulative
    from {{ ref("stg_audience_budget_onetime_donor_count_budget_monthly") }}
    union all
    select
        date_day,
        interval_type,
        donor_audience,
        join_source,
        onetime_donor_count_budget,
        onetime_new_donor_count_budget,
        onetime_donor_count_budget_cumulative,
        onetime_new_donor_count_cumulative
    from {{ ref("stg_audience_budget_onetime_donor_count_budget_yearly") }}

{% endmacro %}
