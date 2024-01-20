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

/* note: these models take the longest to build -- good candidate for intermediate */

{{ config(
    materialized='incremental',
    unique_key='unique_id',
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["interval_type"]
)}}
    
with base as (    
    select
        /* dimensions: date_day, interval_type, donor_audience, channel */
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

        /* total donor counts */
        count(distinct person_id) as total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        count(distinct 
            case when is_first_transaction_this_fy then
            person_id end
        ) as unique_total{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,

        /* new donor counts */
        count(
            distinct case
                when
                    donor_loyalty = 'new_donor' 
                    and is_first_transaction_this_fy
                then person_id
            end
        ) as unique_newFY{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        count(
            case
                when
                    {% if interval == 'day'%}
                    join_date = date_spine_with_audience_and_channel.date_day
                    {% else %}
                    date_trunc(join_date, {{ interval }}) = date_trunc(date_spine_with_audience_and_channel.date_day, {{ interval }}) 
                    {% endif %}
                then person_id
            end
        ) as new{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,

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
        ) as unique_retained{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,

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
        ) as unique_retained3{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,

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
        ) as unique_reinstated{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,

        /* active and lapsed donor counts */
        count(
            distinct case when person_and_transaction.donor_engagement = 'active' then person_id end
        ) as active{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        count(
            distinct case when person_and_transaction.donor_engagement = 'lapsed' then person_id end
        ) as lapsed{% if frequency == 'recurring' %}_recur_{% else %}_onetime_{% endif %}donor_counts,
        

    from {{ref(cross_join)}} as date_spine_with_audience_and_channel
    left join {{ ref(person_and_transaction) }} person_and_transaction
    on date_spine_with_audience_and_channel.date_day = person_and_transaction.date_day
    and date_spine_with_audience_and_channel.donor_audience = person_and_transaction.donor_audience
    and date_spine_with_audience_and_channel.channel = person_and_transaction.channel
    where recurring{% if frequency == 'recurring' %}= true{% else %} = false{% endif %}
    group by 1, 2, 3, 4
),

add_surrogate as (
select 
{{dbt_utils.generate_surrogate_key(['date_day', "interval_type", "donor_audience", "channel"])}} as unique_id,
*
from base
 {% if target.name != 'prod' %}
where date_day >= date_sub(current_date(), interval 1 year)
{% else %}
{% endif %}
)

select * from add_surrogate

{% if is_incremental() %}

    {% if interval == 'day' %}
     -- pulls in all records on same day or after the latest day 
     where date_day >= (select max(date_day) from {{ this }})

    {% else %}
    -- pulls in all records from previous month and current {{interval}}
    -- (since we typically have a date_day) of last day of the {{interval}}
    where date_day >= (select date_sub(max(date_day), interval 1 {{interval}}) from {{this}})

    {% endif %}

{% endif %}





{% endmacro %}
