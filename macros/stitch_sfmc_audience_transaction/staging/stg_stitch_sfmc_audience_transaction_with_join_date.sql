{% macro create_stg_stitch_sfmc_audience_transaction_with_join_date(
    reference_0_name="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    reference_1_name="stg_stitch_sfmc_audience_transaction_first_gift"
) %}
    select
        union_transaction.transaction_date_day as transaction_date_day,
        union_transaction.person_id as person_id,
        union_transaction.coalesced_audience as donor_audience,
        union_transaction.channel as channel, -- should be join source 
        union_transaction.gift_size_string,
        union_transaction.recurring,
        first_gift.join_month_year_date as join_month_year_date,
        first_gift.join_gift_size_string as join_gift_size_string,
        union_transaction.amount as amount
    from {{ ref(reference_0_name) }} as union_transaction
    inner join
        {{ ref(reference_1_name) }} as first_gift
        on union_transaction.person_id = first_gift.person_id
    where union_transaction.recurring = true
{% endmacro %}
