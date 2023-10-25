{% macro create_stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction(
    audience="stg_stitch_sfmc_arc_audience_unioned",
    first_gift="stg_stitch_sfmc_audience_transaction_first_gift",
    transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    donor_engagement="stg_stitch_sfmc_audience_transaction_person_with_donor_engagement"
) %}
    select
        audience.date_day as date_day,
        audience.person_id as person_id,
        audience.donor_audience as donor_audience,
        transactions.donor_loyalty as donor_loyalty,
        donor_engagement.donor_engagement as donor_engagement,
        transactions.gift_size_string as gift_size_str,
        first_gift.first_gift_join_source as join_source,
        first_gift.join_gift_size_string as join_amount_str,
        first_gift.join_gift_size_string_recur as join_amount_str_recur,
        first_gift.join_month_year_date as join_month_year_str
    from {{ ref(audience) }} as audience
    left join
        {{ ref(first_gift) }} as first_gift on audience.person_id = first_gift.person_id
    left join
        {{ ref(transactions) }} as transactions
        on audience.date_day = transactions.transaction_date_day
        and audience.person_id = transactions.person_id
    left join
        {{ ref(donor_engagement) }} as donor_engagement
        on audience.date_day = donor_engagement.date_day
        and audience.person_id = donor_engagement.person_id

{% endmacro %}
