-- fmt: off
{% macro util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval(
    frequency,
    interval
) %}
    {% if frequency not in ['recurring', 'onetime'] %}
        {{ exceptions.raise_compiler_error("'frequency' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'recurring' or 'onetime', got " ~ frequency) }}
    {% endif %}
    {% if interval not in ['day', 'week','month','year'] %}
        {{ exceptions.raise_compiler_error("'interval' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'day', 'week', 'month', or 'year', got " ~ interval) }}
    {% endif %}

    {{ config(
    materialized='table',
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["interval_type"]
)}}

with base as (
    select 
    unique_id,
    date_day,
    {{
            dbt_arc_functions.get_fiscal_year(
                "date_day", var("fiscal_year_start")
            )
        }} as fiscal_year,
    interval_type,
    donor_audience,
    channel,
    total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
    unique_totalFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
    new_to_FY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
    new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
    unique_newFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
    retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
    retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
    reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
    unique_retainedFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
    unique_retained3FY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
    unique_reinstatedFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
    active{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
    lapsed{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts
from 
    {% if interval == 'day' and frequency == 'onetime'%}
    {{ref("stg_audience_transaction_onetime_donor_counts_daily_rollup")}}
    {% elif interval == 'month' and frequency == 'onetime'%}
    {{ref("stg_audience_transaction_onetime_donor_counts_monthly_rollup")}}
    {% elif interval == 'year' and frequency == 'onetime'%}
    {{ref("stg_audience_transaction_onetime_donor_counts_yearly_rollup")}}
    {% elif interval == 'day' and frequency == 'recurring'%}
    {{ref("stg_audience_transaction_recur_donor_counts_daily_rollup")}}
    {% elif interval == 'month' and frequency == 'recurring'%}
    {{ref("stg_audience_transaction_recur_donor_counts_monthly_rollup")}}
    {% elif interval == 'year' and frequency == 'recurring'%}
    {{ref("stg_audience_transaction_recur_donor_counts_yearly_rollup")}}
    {% endif %}
)

    select
        unique_id,
        date_day,
        interval_type,
        fiscal_year,
        donor_audience,
        channel,
        total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts, 
        new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        new_to_FY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        active{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        lapsed{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        sum(unique_totalFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(unique_newFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as new_to_fy{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(unique_retainedFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(unique_retained3FY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(unique_reinstatedFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative
    from base 
    window w as (
        partition by fiscal_year, donor_audience, channel
        order by date_day
    )
{% endmacro %}
