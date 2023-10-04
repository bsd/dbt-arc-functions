{% macro create_mart_arc_revenue_recur_donor_counts_by_gift_size(
    daily='stg_stitch_sfmc_arc_recur_donor_counts_by_gift_size_daily',
    monthly='stg_stitch_sfmc_arc_recur_donor_counts_by_gift_size_monthly',
    yearly='stg_stitch_sfmc_arc_recur_donor_counts_by_gift_size_yearly'
) %}
    select
        date_day,
        interval_type,
        gift_size,
        donor_counts
    from {{ ref(daily) }}
    union all
    select
        date_day,
        interval_type,
        gift_size,
        donor_counts
    from {{ ref(monthly) }}
    union all
    select
        date_day,
        interval_type,
        gift_size,
        donor_counts
    from {{ ref(yearly) }}

{% endmacro %}
