{% macro create_stg_audience_donors_by_day(
    first_gift="stg_stitch_sfmc_parameterized_audience_transaction_first_gift",
    transaction_enriched="stg_stitch_sfmc_parameterized_audience_transactions_enriched",
    donor_engagement="stg_stitch_sfmc_donor_engagement_by_date_day",
    donor_loyalty="stg_stitch_sfmc_donor_loyalty_start_and_end",
    donor_audience="stg_audience_donor_audience_by_day"
) %}
    {{
        config(
            materialized="table",
            cluster_by="recurring",
        )
    }}

    select
        coalesce(donor_engagement.date_day, donor_audience.date_day, donor_loyalty.date_day) as date_day,
        coalesce(donor_engagement.person_id, donor_audience.person_id, donor_loyalty.person_id) as person_id,
        donor_audience.donor_audience,
        donor_loyalty.donor_loyalty,
        donor_engagement.donor_engagement,
        transactions.nth_transaction_this_fiscal_year,
        transactions.recurring,
        transactions.gift_size_string as gift_size_str,
        transactions.channel,
        first_gift.first_gift_join_source as join_source,
        first_gift.join_gift_size_string as join_amount_str,
        first_gift.join_gift_size_string_recur as join_amount_str_recur,
        first_gift.join_month_year_date,
        first_gift.join_month_year_str,
        first_gift.first_gift_recur_status as join_recur_status,
        first_gift.first_gift_donor_audience as join_donor_audience
    from {{ ref(donor_audience)}} as donor_audience
    left join {{ ref(donor_engagement) }} as donor_engagement
        on donor_audience.person_id = donor_engagement.person_id 
        and donor_engagement.date_day = donor_audience.date_day
    left join {{ref(donor_loyalty)}} as donor_loyalty
        on donor_audience.person_id = donor_loyalty.person_id 
        and donor_audience.date_day between donor_loyalty.start_date and donor_loyalty.end_date 
    left join {{ ref(transactions) }} as transactions
        on donor_audience.person_id = transactions.person_id
        and donor_audience.date_day = transactions.transaction_date_day
    left join
        {{ ref(first_gift) }} as first_gift
        on donor_engagement.person_id = first_gift.person_id

{% endmacro %}
