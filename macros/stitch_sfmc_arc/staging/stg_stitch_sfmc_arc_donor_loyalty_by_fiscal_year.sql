{% macro create_stg_stitch_sfmc_arc_donor_loyalty_by_fiscal_year(
    audience_union_transaction_joined="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched"
) %}

    with donor_loyalty_counts as (

     select
        person_id,
        fiscal_year,
        min(transaction_date_day) as start_date,
        date_sub(
            date(concat(fiscal_year, '-', '{{ var('fiscal_year_start') }}')),
            interval 1 day
        ) as end_date,

        row_number() over (partition by person_id order by fiscal_year desc) as row_num
    from {{ ref(audience_union_transaction_joined) }}
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
            from {{ ref(audience_union_transaction_joined) }}
            group by person_id, fiscal_year
        )
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

{% endmacro %}
