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
    
    select
        date_day,
        fiscal_year,
        interval_type,
        donor_audience,
        channel,
        total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        new_donor_loyalty_{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        unique_newFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        active{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        lapsed{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        sum(unique_total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(unique_newFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as newFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(unique_retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(unique_retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(unique_reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over w as reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative
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
    
    as intermediate_rollup
    window w as (
        partition by fiscal_year, donor_audience, channel
        order by date_day
        rows between unbounded preceding and current row
    )
{% endmacro %}
