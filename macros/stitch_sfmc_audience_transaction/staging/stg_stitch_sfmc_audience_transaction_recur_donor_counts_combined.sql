{% macro create_stg_stitch_sfmc_audience_transaction_recur_donor_counts_combined(
    daily='stg_stitch_sfmc_audience_transaction_recur_donor_counts_daily',
    monthly='stg_stitch_sfmc_audience_transaction_recur_donor_counts_monthly',
    yearly='stg_stitch_sfmc_audience_transaction_recur_donor_counts_yearly'
) %}
    select
        date_day,
        interval_type,
        donor_audience,
        platform,
        total_recur_donor_counts,
        new_recur_donor_counts,
        retained_recur_donor_counts,
        retained3_recur_donor_counts,
        active_recur_donor_counts,
        lapsed_recur_donor_counts,
    from {{ ref(daily) }}
    union all
    select
        date_day,
        interval_type,
        donor_audience,
        platform,
        total_recur_donor_counts,
        new_recur_donor_counts,
        retained_recur_donor_counts,
        retained3_recur_donor_counts,
        active_recur_donor_counts,
        lapsed_recur_donor_counts,
    from {{ ref(monthly) }}
    union all
    select
        date_day,
        interval_type,
        donor_audience,
        platform,
        total_recur_donor_counts,
        new_recur_donor_counts,
        retained_recur_donor_counts,
        retained3_recur_donor_counts,
        active_recur_donor_counts,
        lapsed_recur_donor_counts,
    from {{ ref(yearly) }}

{% endmacro %}
