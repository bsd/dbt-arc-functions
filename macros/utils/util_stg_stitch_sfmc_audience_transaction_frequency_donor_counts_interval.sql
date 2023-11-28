-- fmt: off
{% macro util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval(
    frequency,
    interval,
    person_and_transaction="stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction"
) %}
    {% if frequency not in ['recurring', 'onetime'] %}
        {{ exceptions.raise_compiler_error("'frequency' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'recurring' or 'onetime', got " ~ frequency) }}
    {% endif %}
    {% if interval not in ['day', 'week','month','year'] %}
        {{ exceptions.raise_compiler_error("'interval' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'day', 'week', 'month', or 'year', got " ~ interval) }}
    {% endif %}
    with
        date_spine as (
            {% set min_date_query %}
        SELECT min(date_day) FROM {{ ref(person_and_transaction) }}
            {% endset %}
            {% set min_date_results = run_query(min_date_query) %}
            {% if execute %}
                {% set min_date %}'{{min_date_results.columns[0].values()[0]}}'{% endset %}
            {% else %} {% set min_date = "2020-01-01" %}
            {% endif %}

            {% set max_date_query %}
        SELECT max(date_day) FROM {{ ref(person_and_transaction) }}
            {% endset %}
            {% set max_date_results = run_query(max_date_query) %}
            {% if execute %}
                {% set max_date %}'{{max_date_results.columns[0].values()[0]}}'{% endset %}
            {% else %} {% set max_date = "2020-01-01" %}
            {% endif %}

            {{
                dbt_utils.date_spine(
                    datepart="day", start_date=min_date, end_date=max_date
                )
            }}

        ),
        date_spine_with_audience_and_platform as (
            select
                date_day,
                donor_audience,
                channel as platform -- from best_guess_inbound_channel
            from date_spine
            cross join (
                select distinct donor_audience from {{ ref(person_and_transaction) }}
            )
            cross join (
                select distinct channel from {{ ref(person_and_transaction) }}
            )
        ),
        intermediate_rollup as (
            select
                {% if interval == 'day' %} date_spine_with_audience_and_platform.date_day as date_day,
                {% else %} last_day(date_spine_with_audience_and_platform.date_day, {{ interval }}) as date_day,
                {% endif %}
                {% if interval == 'day' %} 'daily' as interval_type,
                {% elif interval == 'week' %} 'weekly' as interval_type,
                {% elif interval == 'month' %} 'monthly' as interval_type,
                {% elif interval == 'year' %} 'yearly' as interval_type,
                {% endif %}
                date_spine_with_audience_and_platform.donor_audience,
                date_spine_with_audience_and_platform.platform, -- from best_guess_inbound_channel
                {{
                    dbt_arc_functions.get_fiscal_year(
                        "date_spine_with_audience_and_platform.date_day", var("fiscal_year_start")
                    )
                }} as fiscal_year,
                count(
                    distinct case
                        when
                            recurring
                            {% if frequency == 'recurring' %} = true
                            {% else %} = false
                            {% endif %}
                        then person_id
                    end
                ) as total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'new_donor' and recurring
                            {% if frequency == 'recurring' %} = true
                            {% else %} = false
                            {% endif %}
                        then person_id
                    end
                ) as new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'retained_donor' and recurring
                            {% if frequency == 'recurring' %} = true
                            {% else %} = false
                            {% endif %}
                        then person_id
                    end
                ) as retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'retained_3+_donor' and recurring
                            {% if frequency == 'recurring' %} = true
                            {% else %} = false
                            {% endif %}
                        then person_id
                    end
                ) as retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'reactivated_donor' and recurring
                            {% if frequency == 'recurring' %} = true
                            {% else %} = false
                            {% endif %}
                        then person_id
                    end
                ) as reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
                count(
                    distinct case when donor_engagement = 'active' then person_id end
                ) as active{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
                count(
                    distinct case when donor_engagement = 'lapsed' then person_id end
                ) as lapsed{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
                count(
                    distinct case
                        when
                            recurring
                            {% if frequency == 'recurring' %} = true
                            {% else %} = false
                            {% endif %} and nth_transaction_this_fiscal_year = 1
                        then person_id
                    end
                ) as unique_total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'new_donor' and recurring
                            {% if frequency == 'recurring' %} = true
                            {% else %} = false
                            {% endif %} and nth_transaction_this_fiscal_year = 1
                        then person_id
                    end
                ) as unique_new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'retained_donor' and recurring
                            {% if frequency == 'recurring' %} = true
                            {% else %} = false
                            {% endif %} and nth_transaction_this_fiscal_year = 1
                        then person_id
                    end
                ) as unique_retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'retained_3+_donor' and recurring
                            {% if frequency == 'recurring' %} = true
                            {% else %} = false
                            {% endif %} and nth_transaction_this_fiscal_year = 1
                        then person_id
                    end
                ) as unique_retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
                count(
                    distinct case
                        when
                            donor_loyalty = 'reactivated_donor' and recurring
                            {% if frequency == 'recurring' %} = true
                            {% else %} = false
                            {% endif %} and nth_transaction_this_fiscal_year = 1
                        then person_id
                    end
                ) as unique_reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
            from date_spine_with_audience_and_platform
            left join {{ ref(person_and_transaction) }} person_and_transaction
            on date_spine_with_audience_and_platform.date_day = person_and_transaction.date_day
            and date_spine_with_audience_and_platform.donor_audience = person_and_transaction.donor_audience
            and date_spine_with_audience_and_platform.platform = person_and_transaction.channel
            group by 1, 2, 3, 4, 5
        )
    select
        date_day,
        fiscal_year,
        interval_type,
        donor_audience,
        platform,
        total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        active{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        lapsed{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        sum(unique_total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(unique_new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(unique_retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(unique_retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative,
        sum(unique_reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts) over (
            partition by fiscal_year, interval_type, donor_audience, platform
            order by date_day
        ) as reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts_cumulative
    from intermediate_rollup
{% endmacro %}
