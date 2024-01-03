{% macro create_stg_audience_transaction_recur_donor_counts_combined(
    daily="stg_audience_transaction_recur_donor_counts_daily",
    monthly="stg_audience_transaction_recur_donor_counts_monthly",
    yearly="stg_audience_transaction_recur_donor_counts_yearly"
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
