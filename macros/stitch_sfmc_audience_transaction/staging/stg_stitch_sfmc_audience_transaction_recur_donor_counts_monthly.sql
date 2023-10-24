{% macro create_stg_stitch_sfmc_audience_transaction_recur_donor_counts_monthly() %}
    select
        last_day(date_day, month) as date_day,
        'monthly' as interval_type,
        donor_audience,
        join_source as platform,
        count(
            distinct case when gift_size_str is not null then person_id end
        ) as total_recur_donor_counts,
        count(
            distinct case
                when donor_loyalty = 'new_donor' and gift_size_str is not null
                then person_id
            end
        ) as new_recur_donor_counts,
        count(
            distinct case
                when donor_loyalty = 'retained_donor' and gift_size_str is not null
                then person_id
            end
        ) as retained_recur_donor_counts,
        count(
            distinct case
                when donor_loyalty = 'retained_3+_donor' and gift_size_str is not null
                then person_id
            end
        ) as retained3_recur_donor_counts,
        count(
            distinct case
                when donor_loyalty = 'reactivated_donor' and gift_size_str is not null
                then person_id
            end
        ) as reinstated_recur_donor_counts,
        count(
            distinct case
                when donor_engagement = 'active' and gift_size_str is not null
                then person_id
            end
        ) as active_recur_donor_counts,
        count(
            distinct case
                when donor_engagement = 'lapsed' and gift_size_str is not null
                then person_id
            end
        ) as lapsed_recur_donor_counts,

    from
        {{
            ref(
                "stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction"
            )
        }}

    where donor_audience = 'recurring'
    group by 1, 2, 3, 4

{% endmacro %}
