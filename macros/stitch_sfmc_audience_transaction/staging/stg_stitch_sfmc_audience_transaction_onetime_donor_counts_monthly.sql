{% macro create_stg_stitch_sfmc_audience_transaction_onetime_donor_counts_actuals_rollup(
    reference_name="stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction"
) %}
    with
        monthly_rollup as (
            select
                last_day(date_day, month) as date_day,
                'monthly' as interval_type,
                donor_audience,
                join_source as platform,
                {{
                    dbt_arc_functions.get_fiscal_year(
                        "date_day", var("fiscal_year_start")
                    )
                }} as fiscal_year,
                count(distinct person_id) as total_onetime_donor_counts,
                count(
                    distinct case when donor_loyalty = 'new_donor' then person_id end
                ) as new_onetime_donor_counts,
                count(
                    distinct case
                        when donor_loyalty = 'retained_donor' then person_id
                    end
                ) as retained_onetime_donor_counts,
                count(
                    distinct case
                        when donor_loyalty = 'retained_3+_donor' then person_id
                    end
                ) as retained3_onetime_donor_counts,
                count(
                    distinct case
                        when donor_loyalty = 'reactivated_donor' then person_id
                    end
                ) as reinstated_onetime_donor_counts,
                count(
                    distinct case when donor_engagement = 'active' then person_id end
                ) as active_onetime_donor_counts,
                count(
                    distinct case when donor_engagement = 'lapsed' then person_id end
                ) as lapsed_onetime_donor_counts,

            from {{ ref(reference_name) }}
            where donor_audience != 'recurring'
            group by 1, 2, 3, 4, 5
        )
    select
        date_day,
        fiscal_year,
        interval_type,
        donor_audience,
        platform,
        total_onetime_donor_counts,
        new_onetime_donor_counts,
        retained_onetime_donor_counts,
        retained3_onetime_donor_counts,
        reinstated_onetime_donor_counts,
        active_onetime_donor_counts,
        lapsed_onetime_donor_counts,
        sum(total_onetime_donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as total_onetime_donor_counts_cumulative,
        sum(new_onetime_donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as new_onetime_donor_counts_cumulative,
        sum(retained_onetime_donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as retained_onetime_donor_counts_cumulative,
        sum(retained3_onetime_donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as retained3_onetime_donor_counts_cumulative,
        sum(reinstated_onetime_donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as reinstated_onetime_donor_counts_cumulative,
        sum(active_onetime_donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as active_onetime_donor_counts_cumulative,
        sum(lapsed_onetime_donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as lapsed_onetime_donor_counts_cumulative
    from monthly_rollup
{% endmacro %}
