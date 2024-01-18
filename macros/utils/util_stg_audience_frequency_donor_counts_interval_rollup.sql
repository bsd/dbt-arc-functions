-- fmt: off
{% macro util_stg_audience_transaction_frequency_donor_counts_interval_rollup(
    frequency,
    interval,
    person_and_transaction="stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction",
    cross_join="stg_audience_channel_by_day_cross_join"
) %}
    {% if frequency not in ['recurring', 'onetime'] %}
        {{ exceptions.raise_compiler_error("'frequency' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'recurring' or 'onetime', got " ~ frequency) }}
    {% endif %}
    {% if interval not in ['day', 'month','year'] %}
        {{ exceptions.raise_compiler_error("'interval' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'day', 'week', 'month', or 'year', got " ~ interval) }}
    {% endif %}
    
    
    select
        {% if interval == 'day' %} date_spine_with_audience_and_channel.date_day,
        {% else %} last_day(date_spine_with_audience_and_channel.date_day, {{ interval }}) as date_day,
        {% endif %}
        {% if interval == 'day' %} 'daily' as interval_type,
        {% elif interval == 'week' %} 'weekly' as interval_type,
        {% elif interval == 'month' %} 'monthly' as interval_type,
        {% elif interval == 'year' %} 'yearly' as interval_type,
        {% endif %}
        date_spine_with_audience_and_channel.donor_audience,
        date_spine_with_audience_and_channel.channel, 
        {{
            dbt_arc_functions.get_fiscal_year(
                "date_spine_with_audience_and_channel.date_day", var("fiscal_year_start")
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
        ) as new_donor_loyalty_{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
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
            distinct case when person_and_transaction.donor_engagement = 'active' then person_id end
        ) as active{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        count(
            distinct case when person_and_transaction.donor_engagement = 'lapsed' then person_id end
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
        ) as unique_newFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        count(
            distinct case
                when
                    {% if interval == 'day'%}
                    join_date = date_spine_with_audience_and_channel.date_day
                    {% elif interval == 'month'%}
                    join_date 
                        between date_trunc(date_spine_with_audience_and_channel.date_day, {{ interval }}) 
                        and last_day(date_spine_with_audience_and_channel.date_day, {{ interval }}) 
                    {% elif interval == 'year' %}
                    extract(year from join_date) = extract(year from date_spine_with_audience_and_channel.date_day)
                    {% endif %}
                    and recurring
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
    from {{ref(cross_join)}} as date_spine_with_audience_and_channel
    left join {{ ref(person_and_transaction) }} person_and_transaction
    on date_spine_with_audience_and_channel.date_day = person_and_transaction.date_day
    and date_spine_with_audience_and_channel.donor_audience = person_and_transaction.donor_audience
    and date_spine_with_audience_and_channel.channel = person_and_transaction.channel
    group by 1, 2, 3, 4, 5

{% endmacro %}
