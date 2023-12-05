{% macro create_stg_stitch_sfmc_arc_revenue_and_donor_count_by_lifetime_gifts(
    reference_name="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched"
) %}

    with
        base as (
            select
                transaction_date_day as transaction_date_day,
                coalesced_audience as donor_audience,
                channel,
                person_id,
                sum(gift_count) as gift_count,
                sum(amount) as amount
            from {{ ref(reference_name) }}
            where recurring = true
            group by 1, 2, 3, 4
            order by 1, 2, 3, 4

        ),
        cumulative_base as (
            select
                transaction_date_day,
                donor_audience,
                channel,
                person_id,
                amount,
                sum(gift_count) over (partition by person_id
                    order by transaction_date_day
                ) as cumulative_gift_count
            from base
            group by
                transaction_date_day,
                donor_audience,
                channel,
                person_id
                amount,
                gift_count
        )

    , add_string as (
        select *,
        case
            when cumulative_gift_count < 6
            then "less than 6"
            when cumulative_gift_count between 6 and 12
            then "6-12"
            when cumulative_gift_count between 13 and 24
            then "13-24"
            when cumulative_gift_count between 25 and 36
            then "25-36"
            else "37+"
        end as recurring_gift_cumulative_str
        from cumulative_base

    )

    select
        transaction_date_day,
        donor_audience,
        channel,
        recurring_gift_cumulative_str,
        count(distinct person_id) as donors,
        sum(amount) as amount
    from add_string
    group by 1, 2, 3, 4

{% endmacro %}
