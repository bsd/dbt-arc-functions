{% macro create_stg_audience_donors_by_day(
    first_gift="stg_audience_parameterized_transaction_first_gift",
    first_fy="stg_audience_first_transaction_this_fy",
    donor_engagement="stg_audience_donor_engagement_by_date_day",
    donor_loyalty="stg_audience_donor_loyalty_start_and_end",
    donor_audience="stg_audience_donor_audience_by_day_unioned"
) %}

   

    select
        donor_audience.date_day as date_day,
        coalesce(
            donor_engagement.person_id,
            donor_audience.person_id,
            donor_loyalty.person_id
        ) as person_id,
        donor_audience.donor_audience,
        donor_loyalty.donor_loyalty,
        donor_engagement.donor_engagement,
        first_fy.first_transaction_this_fiscal_year,
        first_gift.first_gift_join_source as channel,
        first_gift.first_transaction_date as join_date,
        first_gift.first_gift_recur_status as recurring
    from {{ ref(donor_audience) }} as donor_audience
    full outer join
        {{ ref(donor_engagement) }} as donor_engagement
        on donor_audience.person_id = donor_engagement.person_id
        and donor_engagement.date_day = donor_audience.date_day
    full outer join
        {{ ref(donor_loyalty) }} as donor_loyalty
        on donor_audience.person_id = donor_loyalty.person_id
        and donor_audience.date_day
        between donor_loyalty.start_date and donor_loyalty.end_date
    left join
        {{ref(first_fy)}} first_fy
        on donor_audience.person_id = first_fy.person_id
        and donor_audience.date_day = first_fy.transaction_date_day
    left join
        {{ ref(first_gift) }} as first_gift
        on donor_engagement.person_id = first_gift.person_id

{% endmacro %}
