{% macro create_stg_stitch_sfmc_audience_transaction_recur_donor_counts_monthly() %}
    select
        last_day(date_day, month) as date_day,
        'monthly' as interval_type,
        donor_audience,
        join_source as platform,
        count(person_id) as total_recur_donor_counts,
        count(case when donor_loyalty = 'new_donor' then 1 end) as new_recur_donor_counts,
        count(
            case when donor_loyalty = 'retained_donor' then 1 end
        ) as retained_recur_donor_counts,
        count(
            case when donor_loyalty = 'retained_3+_donor' then 1 end
        ) as retained3_recur_donor_counts,
        count(
            case when donor_loyalty = 'reactivated_donor' then 1 end
        ) as reinstated_recur_donor_counts,
        count(
            case when donor_engagement = 'active' then 1 end
        ) as active_recur_donor_counts,
        count(
            case when donor_engagement = 'lapsed' then 1 end
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
