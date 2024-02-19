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
    new_to_fy{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
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
),

distinct_audiences as (
    select distinct donor_audience from base
),

distinct_channels as (
    select distinct channel from base
),

distinct_days as (
    select distinct date_day from base
),

cross_join as (
    select 
        date_day,
        donor_audience,
        channel 
    from distinct_days 
    cross join distinct_channels
    cross join distinct_audiences
),

true_cumulative as (
    select 
        cross_join.date_day,
        base.fiscal_year,
        base.interval_type,
        cross_join.donor_audience,
        cross_join.channel,
        sum(base.unique_totalFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(base.unique_newFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as new_to_fy{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(base.new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(base.unique_retainedFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(base.unique_retained3FY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(base.unique_reinstatedFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative
    from cross_join 
    full outer join base using (date_day, donor_audience, channel)
    window w as (
        partition by fiscal_year, donor_audience, channel
        order by date_day
    )
)

    select
        true_cumulative.date_day,
        true_cumulative.interval_type,
        true_cumulative.fiscal_year,
        true_cumulative.donor_audience,
        true_cumulative.channel,
        base.total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts, 
        base.new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        base.new_to_fy{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        base.retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        base.retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        base.reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        base.active{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        base.lapsed{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        true_cumulative.total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        true_cumulative.new_to_fy{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        true_cumulative.new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        true_cumulative.retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        true_cumulative.retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        true_cumulative.reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative
    from true_cumulative 
    full outer join base using (date_day, donor_audience, channel)
    
{% endmacro %}
