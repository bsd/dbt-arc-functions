{% macro create_stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction(
    first_gift="stg_stitch_sfmc_parameterized_audience_transaction_first_gift",
    transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    donor_engagement="stg_stitch_sfmc_donor_engagement_by_date_day"
) %}
{{ config(materialized="table",
         partition_by={
            "field": "date_day",
            "data_type": "date",
            "granularity": "day",
        }) }}

with

    transaction_per_day as (
        select
            transaction_date_day,
            person_id,
            donor_loyalty,
            coalesced_audience as donor_audience,
            case
                when nth_transaction_this_fiscal_year = 1 then true else false
            end as is_first_transaction_this_fy,
            recurring,
            gift_size_string,
            channel,
            row_number() over (
                partition by transaction_date_day, person_id order by transaction_id asc
            ) as dup
        from {{ ref(transactions) }}
        qualify dup = 1
    ),

    transaction_per_day_with_next_date as (
        select
            *,
            lead(transaction_date_day) over (
                partition by person_id order by transaction_date_day
            ) as next_transaction_date
        from transaction_per_day

    ),

    datespine as (

        select date
        from
            unnest(
                generate_date_array(
                    (select min(transaction_date_day) from transaction_per_day),
                    ifnull(
                        (select max(transaction_date_day) from transaction_per_day),
                        current_date()
                    )
                )
            ) as date

    ),

    transaction_datespine as (
        select
            datespine.date as date_day,
            transaction_per_day_with_next_date.person_id,
            transaction_per_day_with_next_date.donor_loyalty,
            transaction_per_day_with_next_date.donor_audience,
            transaction_per_day_with_next_date.is_first_transaction_this_fy,
            transaction_per_day_with_next_date.recurring,
            transaction_per_day_with_next_date.gift_size_string,
            transaction_per_day_with_next_date.channel
        from datespine
        left join
            transaction_per_day_with_next_date
            on datespine.date >= transaction_per_day_with_next_date.transaction_date_day
            and datespine.date
            < transaction_per_day_with_next_date.next_transaction_date
        {% if is_incremental() %}
        -- pulls in all records within 7 days of max day
        where
            transaction_per_day_with_next_date.transaction_date_day >= (
                select date_sub(max(transaction_date_day), interval 7 day)
                from transaction_per_day_with_next_date
            )
        {% endif %}
    ),

    final as (

        select
            {{
                dbt_utils.generate_surrogate_key(
                    ["donor_engagement.date_day", "donor_engagement.person_id"]
                )
            }} as unique_id,
            donor_engagement.date_day,
            donor_engagement.person_id,
            transaction_datespine.donor_audience,
            transaction_datespine.donor_loyalty,
            transaction_datespine.is_first_transaction_this_fy,
            transaction_datespine.recurring,
            donor_engagement.donor_engagement,
            transaction_datespine.gift_size_string as gift_size_str,
            transaction_datespine.channel,
            first_gift.first_gift_join_source as join_source,
            first_gift.join_gift_size_string as join_amount_str,
            first_gift.join_gift_size_string_recur as join_amount_str_recur,
            first_gift.join_month_year_date as join_month_year_str,
            first_gift.first_transaction_date as join_date,
        from transaction_datespine
        left join
            {{ ref(donor_engagement) }} as donor_engagement
            on donor_engagement.person_id = transaction_datespine.person_id
            and donor_engagement.date_day = transaction_datespine.date_day
        left join
            {{ ref(first_gift) }} as first_gift
            on donor_engagement.person_id = first_gift.person_id
    )

select *
from final

{% endmacro %}
