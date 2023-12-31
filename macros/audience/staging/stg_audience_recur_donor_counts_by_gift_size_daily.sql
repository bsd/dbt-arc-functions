{% macro create_stg_audience_recur_donor_counts_by_gift_size_daily(
    audience_transaction="stg_audience_transactions_and_audience_summary"
) %}

    with
        base as (
            select
                transaction_date_day as date_day,
                channel,
                donor_audience,
                gift_size_string as gift_size,
                count(distinct person_id) as donor_counts
            from {{ ref(audience_transaction) }}
            where recurring = true
            group by 1, 2, 3, 4
            order by 1 desc, 4
        )
    select 'daily' as interval_type, *
    from base

{% endmacro %}
