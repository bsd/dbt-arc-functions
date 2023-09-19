{% macro create_stg_stitch_sfmc_arc_audience_union_transaction_joined(
    donor_audience_unioned="stg_stitch_sfmc_arc_audience_unioned",
    donor_engagement_by_day="stg_stitch_sfmc_donor_engagement_by_date_day",
    donor_transaction_enriched="stg_stitch_sfmc_parameterized_audience_transactions_enriched"
) %}

    select
        transaction_enriched.transaction_date_day,
        {{
            dbt_arc_functions.get_fiscal_year(
                "transaction_enriched.transaction_date_day",
                var("fiscal_year_start"),
            )
        }} as fiscal_year,
        transaction_enriched.person_id,
        audience_unioned.donor_audience,
        donor_engagement.donor_engagement,
        transaction_enriched.best_guess_inbound_channel as channel,
        transaction_enriched.inbound_channel as channel_category,
        transaction_enriched.gift_size_string,
        transaction_enriched.recurring,
        transaction_enriched.amount,
        transaction_enriched.gift_count,
    from {{ ref(donor_transaction_enriched) }} transaction_enriched
    left join
        {{ ref(donor_audience_unioned) }} audience_unioned
        on transaction_enriched.transaction_date_day = audience_unioned.date_day
        and transaction_enriched.person_id = audience_unioned.person_id
    left join
        {{ ref(donor_engagement_by_day) }} donor_engagement
        on transaction_enriched.transaction_date_day = donor_engagement.date_day
        and transaction_enriched.person_id = donor_engagement.person_id

{% endmacro %}
