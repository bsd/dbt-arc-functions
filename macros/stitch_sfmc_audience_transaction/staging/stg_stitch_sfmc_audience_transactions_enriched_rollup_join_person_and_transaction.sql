{% macro create_stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction(
    audience="stg_stitch_sfmc_arc_audience_unioned",
    first_gift="stg_stitch_sfmc_audience_transaction_first_gift",
    transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined",
    audience_transaction_jobs_append="stg_stitch_sfmc_customizable_audience_transaction_jobs_append"
) %}
    select
        audience.date_day as date_day,
        audience.person_id as person_id,
        audience.donor_audience as donor_audience,
        transactions.donor_engagement as donor_engagement,
        audience_transaction_jobs_append.donor_loyalty as donor_loyalty,
        transactions.gift_size_string as gift_size_str,
        first_gift.first_gift_join_source as join_source,
        first_gift.join_gift_size_string as join_amount_str,
        first_gift.join_gift_size_string_recur as join_amount_str_recur,
        first_gift.join_month_year_date as join_month_year_str
    from {{ ref(transactions) }} as transactions
    left join
        {{ ref(first_gift) }} as first_gift
        on transactions.person_id = first_gift.person_id
    left join {{ ref(audience) }} as audience
        on audience.date_day = transactions.transaction_date_day
        and audience.person_id = transactions.person_id
    left join
        {{ ref(audience_transaction_jobs_append) }} as audience_transaction_jobs_append
        on transactions.transaction_date_day
        = audience_transaction_jobs_append.transaction_date_day
        and transactions.person_id = audience_transaction_jobs_append.person_id

{% endmacro %}
