{% macro create_stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction(
    audience="stg_stitch_sfmc_arc_audience_unioned",
    first_gift="stg_stitch_sfmc_audience_transaction_first_gift",
    transactions="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched",
    audience_transaction_jobs_append="stg_stitch_sfmc_audience_transaction_jobs_append"
) %}
    with
        base as (
            select
                audience.date_day as date_day,
                audience.person_id as person_id,
                audience.donor_audience as donor_audience,
                transactions.donor_loyalty as donor_loyalty,
                transactions.donor_engagement as donor_engagement,
                transactions.gift_size_string as gift_size_str,
                first_gift.first_gift_join_source as join_source,
                first_gift.join_gift_size_string as join_amount_str,
                first_gift.join_gift_size_string_recur as join_amount_str_recur,
                first_gift.join_month_year_date as join_month_year_str
            from {{ ref(audience) }} as audience
            left join
                {{ ref(first_gift) }} as first_gift
                on audience.person_id = first_gift.person_id
            left join
                {{ ref(transactions) }} as transactions
                on audience.date_day = transactions.transaction_date_day
                and audience.person_id = transactions.person_id
        ),

        base_with_added_last_date as (
            select
                date_day,
                person_id,
                donor_audience,
                donor_loyalty,
                donor_engagement,
                gift_size_str,
                join_source,
                join_amount_str,
                join_amount_str_recur,
                join_month_year_str,
                max(case when donor_loyalty is not null then date_day end) over (
                    partition by person_id order by date_day
                ) as last_time_loyalty,
                max(case when donor_engagement is not null then date_day end) over (
                    partition by person_id order by date_day
                ) as last_time_engagement
            from base
        )
    select
        date_day,
        person_id,
        donor_audience,
        coalesce(
            donor_loyalty,
            max(donor_loyalty) over (partition by person_id, last_time_loyalty)
        ) as donor_loyalty,
        coalesce(
            donor_engagement,
            max(donor_engagement) over (partition by person_id, last_time_engagement)
        ) as donor_engagement,
        gift_size_str,
        join_source,
        join_amount_str,
        join_amount_str_recur,
        join_month_year_str
    from base_with_added_last_date

{% endmacro %}
