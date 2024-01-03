{% macro create_stg_audience_budget_onetime_donor_counts_with_interval_combined(
    daily="stg_audience_budget_onetime_donor_count_daily",
    monthly="stg_audience_budget_onetime_donor_count_monthly",
    yearly="stg_audience_budget_onetime_donor_count_yearly"
) %}

{{
    config(
        materialized='table',
        partition_by={
                "field": "date_day",
                "data_type": "date",
                "granularity": "day",
            },
        cluster_by='interval_type'
    )
}}

    select *
    from {{ ref(daily) }}
    union all
    select *
    from {{ ref(monthly) }}
    union all
    select *
    from {{ ref(yearly) }}

{% endmacro %}
