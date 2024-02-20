{{ config(severity="warn") }}

with
    a as (
        select
            date(
                extract(year from transaction_date_day),
                extract(month from transaction_date_day),
                1
            ) as date_day,
            sum(donors) as recur_donors_a
        from {{ ref("mart_audience_revenue_and_donor_count_recur_by_lifetime_gifts") }}
        group by 1
    ),

    b as (
        select date_day, sum(donor_counts) as recur_donors_b
        from {{ ref("mart_arc_revenue_recur_donor_counts_by_gift_size") }}
        where interval_type = 'monthly'
        group by 1

    ),

    c as (
        select date_day, sum(total_recur_donor_counts) as recur_donors_c
        from {{ ref("mart_audience_budget_with_audience_transaction_recur") }}
        where interval_type = 'monthly'
        group by 1

    ),

    d as (
        select date_day as date_day, sum(total_onetime_donor_counts) as onetime_donors_d
        from {{ ref("mart_audience_budget_with_audience_transaction") }}
        where interval_type = 'monthly'
        group by 1

    ),

    full_join as (
        select
            coalesce(a.date_day, b.date_day, c.date_day, d.date_day) as date_day,
            round(d.onetime_donors_d, 0) onetime_donors_d,
            round(c.recur_donors_c, 0) as recur_donors_c,
            round(b.recur_donors_b, 0) as recur_donors_b,
            round(a.recur_donors_a, 0) as recur_donors_a
        from a
        full join b using (date_day)
        full join c using (date_day)
        full join d using (date_day)

    ),

    issues as (
        select
            date_day,
            sum(
                case when recur_donors_a != recur_donors_b then 1 else 0 end
            ) as a_not_equal_b,
            sum(
                case when recur_donors_a != recur_donors_c then 1 else 0 end
            ) as a_not_equal_c,
            sum(
                case when recur_donors_b != recur_donors_c then 1 else 0 end
            ) as b_not_equal_c
        from full_join
        group by 1
    )

select *
from issues
where
    a_not_equal_b > 0 or a_not_equal_c > 0 or b_not_equal_c > 0

    /* to do: the one model representing 1x donor counts doesn't have another mart for comparision */
    
