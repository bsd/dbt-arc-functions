{{ config(severity="warn") }}

with
    activation_mart as (
        select distinct
            join_month_year_date as join_date, sum(donors_in_cohort) as new_donors
        from {{ ref("mart_audience_1x_activation") }}
        where activation_int = 0
        group by 1
    ),

    onetime_donors as (
        select date_day as join_date, sum(new_onetime_donor_counts) as new_donors
        from {{ ref("mart_audience_budget_with_audience_transaction") }}
        where lower(interval_type) = 'monthly' and new_onetime_donor_counts > 0
        group by 1
    ),

    source_of_truth as (
        select 
        join_month_year_date as join_date, 
        count(distinct person_id) as new_donors
        from {{ref('stg_stitch_sfmc_parameterized_audience_transaction_first_gift')}}
        where first_gift_recur_status = False
        group by 1

    ),

    issues as (

        select
            join_date,
            activation_mart.new_donors as activation_new_donors,
            onetime_donors.new_donors as onetime_new_donors, 
            source_of_truth.new_donors as actual_new_donors
        from activation_mart
        full join onetime_donors using (join_date)
        full join source_of_truth using (join_date)
        where activation_mart.new_donors != source_of_truth.new_donors
        or onetime_donors.new_donors != source_of_truth.new_donors
    )

select join_date, count(*)
from issues
group by 1
