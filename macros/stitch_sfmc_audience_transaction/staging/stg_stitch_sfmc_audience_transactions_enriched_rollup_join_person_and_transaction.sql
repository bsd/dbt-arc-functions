{% macro create_stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction(
    first_gift="stg_stitch_sfmc_parameterized_audience_transaction_first_gift",
    transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    donor_engagement="stg_stitch_sfmc_donor_engagement_by_date_day"
) %}
    {{
        config(
            materialized="table",
            cluster_by="recurring",
        )
    }}
    select
        donor_engagement.date_day as date_day,
        donor_engagement.person_id as person_id,
        last_value(transactions.coalesced_audience ignore nulls) over (
            partition by donor_engagement.person_id
            order by donor_engagement.date_day
            rows between unbounded preceding and current row
        ) as donor_audience,
        transactions.donor_loyalty as donor_loyalty,
        transactions.nth_transaction_this_fiscal_year
        as nth_transaction_this_fiscal_year,
        transactions.recurring as recurring,
        donor_engagement.donor_engagement as donor_engagement,
        transactions.gift_size_string as gift_size_str,
        transactions.channel as channel,  -- from best_guess_inbound_channel
        first_gift.first_gift_join_source as join_source,
        first_gift.join_gift_size_string as join_amount_str,
        first_gift.join_gift_size_string_recur as join_amount_str_recur,
        first_gift.join_month_year_date as join_month_year_str,
    from
        (
            select date_day, person_id, donor_engagement
            from {{ ref(donor_engagement) }}
        ) as donor_engagement
    left join
        (
            select
                person_id,
                transaction_date_day,
                donor_loyalty,
                recurring,
                gift_size_string,
                coalesced_audience,
                channel,  -- from best_guess_inbound_channel
                row_number() over (
                    partition by person_id, fiscal_year order by transaction_date_day
                ) as nth_transaction_this_fiscal_year
            from {{ ref(transactions) }}
        ) as transactions
        on donor_engagement.person_id = transactions.person_id
        and donor_engagement.date_day = transactions.transaction_date_day
    left join
        (
            select
                person_id,
                first_gift_join_source,
                join_gift_size_string,
                join_gift_size_string_recur,
                join_month_year_date
            from {{ ref(first_gift) }}
        ) as first_gift
        on donor_engagement.person_id = first_gift.person_id
{% endmacro %}
