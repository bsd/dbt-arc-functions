{% macro create_stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction(
    reference_0_name="stg_stitch_sfmc_arc_audience_unioned",
    reference_1_name="stg_stitch_sfmc_audience_transaction_first_gift",
    reference_2_name="stg_stitch_sfmc_arc_audience_union_transaction_joined"
) %}
    select
        audience.date_day as date_day,
        audience.person_id as person_id,
        audience.donor_audience as donor_audience,
        transactions.donor_engagement as donor_engagement,
        transactions.gift_size_string as gift_size_str,
        first_gift.first_gift_join_source as join_source,
        first_gift.join_gift_size_string as join_amount_str,
        first_gift.join_month_year_date as join_month_year_str
    from {{ ref(reference_0_name) }} as audience
    left join
        {{ ref(reference_1_name) }} as first_gift
        on audience.person_id = first_gift.person_id
    left join
        {{ ref(reference_2_name) }} as transactions
        on audience.date_day = transactions.transaction_date_day
        and audience.person_id = transactions.person_id

{% endmacro %}
