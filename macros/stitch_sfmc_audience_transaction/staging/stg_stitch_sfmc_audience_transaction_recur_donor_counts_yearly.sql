{% macro create_stg_stitch_sfmc_audience_transaction_recur_donor_counts_yearly() %}
select
    last_day(date_day, year) as date_day,
    'yearly' as interval_type,
    donor_audience,
    join_source as platform,
    count(person_id) as total_recur_donor_counts,
    count(case when donor_loyalty = 'new' then 1 end) as new_recur_donor_counts,
    count(
        case when donor_loyalty = 'retained' then 1 end
    ) as retained_recur_donor_counts,
    count(
        case when donor_loyalty = 'retained3' then 1 end
    ) as retained3_recur_donor_counts,
    count(
        case when donor_engagement = 'active' then 1 end
    ) as active_recur_donor_counts,
    count(
        case when donor_engagement = 'lapsed' then 1 end
    ) as lapsed_recur_donor_counts,

from
    (
        select
            date_day,
            person_id,
            donor_audience,
            donor_engagement,
            donor_loyalty,
            gift_size_str,
            join_source,
            join_amount_str,
            join_month_year_str,
        from {{ ref("stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction") }}
    )

where donor_audience = 'recurring'
group by 1, 2, 3

{% endmacro %}