{% macro util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval(
    frequency,
    interval,
    reference_name="stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction"
) %}
    {% if frequency not in ['recurring', 'onetime'] %}
        {{ exceptions.raise_compiler_error("'frequency' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'recurring' or 'onetime', got " ~ frequency) }}
    {% endif %}
    {% if interval not in ['day', 'week','month','year'] %}
        {{ exceptions.raise_compiler_error("'interval' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'day', 'week', 'month', or 'year', got " ~ interval) }}
    {% endif %}
    with
        intermediate_rollup as (
            select
                {% if interval == 'day' %} date_day,
                {% else %} last_day(date_day, '{{interval}}') as date_day,
                {% endif %}
                {% if interval == 'day' %} 'daily' as interval_type,
                {% elif interval == 'week' %} 'weekly' as interval_type,
                {% elif interval == 'month' %} 'monthly' as interval_type,
                {% elif interval == 'year' %} 'yearly' as interval_type,
                {% endif %}
                donor_audience,
                join_source as platform,
                {{
                    dbt_arc_functions.get_fiscal_year(
                        "date_day", var("fiscal_year_start")
                    )
                }} as fiscal_year,
                count(
                    distinct case when gift_size_str is not null then person_id end
                ) as total_onetime_donor_counts,
                count(
                    distinct case
                        when donor_loyalty = 'new_donor' and gift_size_str is not null
                        then person_id
                    end
                ) as new_onetime_donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'retained_donor'
                            and gift_size_str is not null
                        then person_id
                    end
                ) as retained_onetime_donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'retained_3+_donor'
                            and gift_size_str is not null
                        then person_id
                    end
                ) as retained3_onetime_donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'reactivated_donor'
                            and gift_size_str is not null
                        then person_id
                    end
                ) as reinstated_onetime_donor_counts,
                count(
                    distinct case when donor_engagement = 'active' then person_id end
                ) as active_onetime_donor_counts,
                count(
                    distinct case when donor_engagement = 'lapsed' then person_id end
                ) as lapsed_onetime_donor_counts,
                count(
                    distinct case
                        when
                            gift_size_str is not null
                            and nth_transaction_this_fiscal_year = 1
                        then person_id
                    end
                ) as unique_total_onetime_donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'new_donor'
                            and gift_size_str is not null
                            and nth_transaction_this_fiscal_year = 1
                        then person_id
                    end
                ) as unique_new_onetime_donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'retained_donor'
                            and gift_size_str is not null
                            and nth_transaction_this_fiscal_year = 1
                        then person_id
                    end
                ) as unique_retained_onetime_donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'retained_3+_donor'
                            and gift_size_str is not null
                            and nth_transaction_this_fiscal_year = 1
                        then person_id
                    end
                ) as unique_retained3_onetime_donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'reactivated_donor'
                            and gift_size_str is not null
                            and nth_transaction_this_fiscal_year = 1
                        then person_id
                    end
                ) as unique_reinstated_onetime_donor_counts,

            from {{ ref(reference_name) }}
            where
                recurring
                {% if frequency == 'recurring' %} == true
                {% else %} != true
                {% endif %}
            group by 1, 2, 3, 4
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
        sum(unique_total_onetime_donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as total_onetime_donor_counts_cumulative,
        sum(unique_new_onetime_donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as new_onetime_donor_counts_cumulative,
        sum(unique_retained_onetime_donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as retained_onetime_donor_counts_cumulative,
        sum(unique_retained3_onetime_donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as retained3_onetime_donor_counts_cumulative,
        sum(unique_reinstated_onetime_donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as reinstated_onetime_donor_counts_cumulative
    from intermediate_rollup
{% endmacro %}
