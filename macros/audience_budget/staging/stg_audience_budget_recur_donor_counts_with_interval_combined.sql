{% macro create_stg_audience_budget_recur_donor_counts_with_interval_combined(
    daily="stg_audience_budget_recur_donor_count_daily",
    monthly="stg_audience_budget_recur_donor_count_monthly",
    yearly="stg_audience_budget_recur_donor_count_yearly"
) %}

select *
from {{ ref(daily) }}
union all
select *
from {{ ref(monthly) }}
union all
select *
from {{ ref(yearly) }}

{% endmacro %}
