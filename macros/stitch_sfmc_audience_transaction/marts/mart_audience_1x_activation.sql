{% macro create_mart_audience_1x_activation(
    donors_in_cohort='stg_stitch_sfmc_audience_transaction_first_gift_1x_rollup',
    transactions='stg_stitch_sfmc_audience_transaction_with_first_gift_cohort'
) %}

with rev_by_cohort as (
select 
join_month_year_str,
first_gift_join_source,
join_gift_size_string, 
first_gift_donor_audience,
concat('Act' || month_diff_str ) as activation_str,
sum(amounts) as total_amount,
SUM(amount) OVER (
    PARTITION BY join_month_year_str, first_gift_join_source, join_gift_size_string, first_gift_donor_audience, activation_str
    ORDER BY month_diff_int
  ) AS cumulative_amount
from {{ref(transactions)}} 
where first_gift_recur_status = 'one_time'
group by 1, 2, 3, 4, 5

)

, donors_in_cohort as (
select 
rev_by_cohort.*,
donors_in_cohort.donors_in_cohort,
from rev_by_cohort
left join {{ref(donors_in_cohort) }} donors_in_cohort
on rev_by_cohort.join_month_year_str = donors_in_cohort.join_month_year_str
and rev_by_cohort.first_gift_join_source = donors_in_cohort.first_gift_join_source
and rev_by_cohort.join_gift_size_string = donors_in_cohort.join_gift_size_string
and rev_by_cohort.first_gift_donor_audience = donors_in_cohort.first_gift_donor_audience
where donors_in_cohort.first_gift_recur_status = 'one_time'

)

select
join_month_year_str as join_month_year,
first_gift_join_source as join_source,
join_gift_size_string as join_gift_size,
first_gift_donor_audience as join_donor_audience,
activation_str,
total_amount,
cumulative_amount,
donors_in_cohort
from donors_in_cohort

{% endmacro %}


