{% macro create_stg_stitch_sfmc_audience_transaction_recur_donor_counts_combined() %}
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
    from {{ ref("stg_stitch_sfmc_audience_transaction_recur_donor_counts_daily") }}
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
    from {{ ref("stg_stitch_sfmc_audience_transaction_recur_donor_counts_monthly") }}
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
    from {{ ref("stg_stitch_sfmc_audience_transaction_recur_donor_counts_yearly") }}

{% endmacro %}
