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
                {% else %} last_day(date_day, {{ interval }}) as date_day,
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

            from {{ ref(reference_name) }}
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
