-- fmt: off
{% macro util_stg_audience_transaction_frequency_donor_counts_interval_rollup(
    frequency,
    interval,
    person_and_transaction="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched_digital",
    first_gift="stg_stitch_sfmc_parameterized_audience_transaction_first_gift"
) %}
    {% if frequency not in ['recurring', 'onetime'] %}
        {{ exceptions.raise_compiler_error("'frequency' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'recurring' or 'onetime', got " ~ frequency) }}
    {% endif %}
    {% if interval not in ['day', 'month','year'] %}
        {{ exceptions.raise_compiler_error("'interval' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'day', 'week', 'month', or 'year', got " ~ interval) }}
    {% endif %}

{{ config(
    materialized='table',
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    }
)}}


with relevant_first_gifts as (
        select * from {{ ref(first_gift) }}
        where first_gift_recur_status{% if frequency == 'recurring' %}= true{% else %} = false{% endif %}
),

base as (
    select 
        person_and_transaction.transaction_date_day,
        person_and_transaction.person_id,
        initcap(person_and_transaction.channel) as channel,
        person_and_transaction.donor_audience,
        person_and_transaction.donor_engagement,
        person_and_transaction.donor_loyalty,
        person_and_transaction.is_first_transaction_this_fy,
        relevant_first_gifts.first_gift_join_source as join_source,
        relevant_first_gifts.join_gift_size_string as join_amount_str,
        relevant_first_gifts.join_gift_size_string_recur as join_amount_str_recur,
        relevant_first_gifts.join_month_year_date as join_month_year_str,
        relevant_first_gifts.first_transaction_date as join_date
        from {{ref(person_and_transaction)}} person_and_transaction
        left join
            relevant_first_gifts
            on person_and_transaction.person_id = relevant_first_gifts.person_id
        where recurring{% if frequency == 'recurring' %}= true{% else %} = false{% endif %}
)

    select
            /* dimensions: date_day, interval_type, donor_audience, channel */
        {% if interval == 'day' %} transaction_date_day as date_day,
        {% elif interval == 'month' %}
        date(extract(year from transaction_date_day), extract(month from transaction_date_day), 1) as date_day,
        {% elif interval == 'year' %}
        date(extract(year from transaction_date_day), 1, 1) as date_day,
        {% endif %}
        donor_audience,
        channel, 
            /* total donor counts */
        count(distinct person_id) as total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        count(distinct 
            case when is_first_transaction_this_fy then
            person_id end
        ) as unique_totalFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,

            /* new donor counts */
        count(
            distinct case
                when
                    donor_loyalty = 'new_donor' 
                    and is_first_transaction_this_fy
                then person_id
            end
        ) as unique_newFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        count(distinct 
            case
                when
                    {% if interval == 'day'%}
                    join_date = transaction_date_day
                    {% else %}
                    date_trunc(join_date, {{ interval }}) = date_trunc(transaction_date_day, {{ interval }}) 
                    {% endif %}
                then person_id
            end
        ) as new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,

            /* new donors */
        count(
            distinct case
                when
                    donor_loyalty = 'new_donor' 
                then person_id
            end
        ) as new_to_fy{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,

            /* retained donor counts */
        count(
            distinct case
                when
                donor_loyalty = 'retained_donor'
                then person_id
            end
        ) as retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        count(
            distinct case
                when
                    donor_loyalty = 'retained_donor' and is_first_transaction_this_fy
                then person_id
            end
        ) as unique_retainedFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,

            /* retained 3 years+ donor counts */
        count(
            distinct case
                when
                    donor_loyalty = 'retained_3+_donor'
                then person_id
            end
        ) as retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        count(
            distinct case
                when
                    donor_loyalty = 'retained_3+_donor' and is_first_transaction_this_fy
                then person_id
            end
        ) as unique_retained3FY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,

            /* reinstated donor counts */
        count(
            distinct case
                when
                    donor_loyalty = 'reactivated_donor'
                then person_id
            end
        ) as reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
         count(
            distinct case
                when
                    donor_loyalty = 'reactivated_donor' and is_first_transaction_this_fy
                then person_id
            end
        ) as unique_reinstatedFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,

            /* active and lapsed donor counts */
        count(
            distinct case when donor_engagement = 'active' then person_id end
        ) as active{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        count(
            distinct case when donor_engagement = 'lapsed' then person_id end
        ) as lapsed{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,

    from base
    group by 1, 2, 3


{% endmacro %}
