{% macro create_stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched(
    donor_audience_unioned="stg_stitch_sfmc_arc_audience_unioned",
    donor_engagement_by_day="stg_stitch_sfmc_donor_engagement_by_date_day",
    donor_transaction_enriched="stg_stitch_sfmc_parameterized_audience_transactions_enriched",
    jobs_append="stg_stitch_sfmc_audience_transaction_jobs_append"
) %}


with audience_union_transaction_joined as (

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

)


, donor_loyalty_counts as (

     select
        person_id,
        fiscal_year,
        min(transaction_date_day) as start_date,
        date_sub(
            date(concat(fiscal_year, '-', '{{ var('fiscal_year_start') }}')),
            interval 1 day
        ) as end_date,

        row_number() over (partition by person_id order by fiscal_year desc) as row_num
    from audience_union_transaction_joined
    group by person_id, fiscal_year
    order by person_id, fiscal_year        
    )


    ,    donation_history as (
            select
                person_id,
                fiscal_year,
                lag(fiscal_year) over (
                    partition by person_id order by fiscal_year
                ) as previous_fiscal_year,
                lag(fiscal_year, 2) over (
                    partition by person_id order by fiscal_year
                ) as fiscal_year_before_previous,
                max(transaction_date_day) as last_donation_date
            from audience_union_transaction_joined
            group by person_id, fiscal_year
        )

        , arc_donor_loyalty as (
    select
        donor_loyalty_counts.person_id,
        donor_loyalty_counts.fiscal_year,
        donor_loyalty_counts.start_date,
        donor_loyalty_counts.end_date,
        case
            when donation_history.previous_fiscal_year is null
            then 'new_donor'
            when
                donation_history.previous_fiscal_year
                = donor_loyalty_counts.fiscal_year - 1
                and donation_history.fiscal_year_before_previous is null
            then 'retained_donor'
            when
                donation_history.previous_fiscal_year
                = donor_loyalty_counts.fiscal_year - 1
                and donation_history.fiscal_year_before_previous is not null
            then 'retained_3+_donor'
            when
                donation_history.previous_fiscal_year
                <> donor_loyalty_counts.fiscal_year - 1
            then 'reactivated_donor'
        end as donor_loyalty
    from donor_loyalty_counts
    left join
        donation_history
        on donor_loyalty_counts.person_id = donation_history.person_id
        and donor_loyalty_counts.fiscal_year = donation_history.fiscal_year
    order by donor_loyalty_counts.person_id, donor_loyalty_counts.fiscal_year

),

audience_calculated_alldates as (

    select transaction_date_day, person_id, donor_audience from jobs_append

)

    select
        audience_union_transaction_joined.transaction_date_day,
        audience_union_transaction_joined.fiscal_year,
        audience_union_transaction_joined.person_id,
        audience_union_transaction_joined.donor_audience as audience_unioned,
        audience_calculated_alldates.donor_audience as audience_calculated,
        donor_engagement,
        arc_donor_loyalty.donor_loyalty,
        channel_category,
        channel,
        gift_size_string,
        recurring,
        amount,
        1 as gift_count,
        coalesce(
            audience_union_transaction_joined.donor_audience,
            audience_calculated_alldates.donor_audience
        ) as coalesced_audience,
        case
            when audience_union_transaction_joined.donor_audience is not null
            then 'audience_union_transaction_joined.donor_audience'
            else 'audience_calculated_alldates.donor_audience'
        end as source_column
    from
        audience_union_transaction_joined
    left join
        {{ ref(audience_calculated_alldates) }}
        as audience_calculated_alldates
        on audience_calculated_alldates.transaction_date_day
        = audience_union_transaction_joined.transaction_date_day
        and audience_calculated_alldates.person_id
        = audience_union_transaction_joined.person_id
    left join
         arc_donor_loyalty
        on audience_union_transaction_joined.person_id = arc_donor_loyalty.person_id
        and audience_union_transaction_joined.transaction_date_day
        between arc_donor_loyalty.start_date and arc_donor_loyalty.end_date

{% endmacro %}
