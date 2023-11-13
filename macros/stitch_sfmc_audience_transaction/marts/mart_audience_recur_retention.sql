{% macro create_mart_audience_recur_retention(
    donors_in_cohort='stg_stitch_sfmc_audience_transaction_first_gift_recur_rollup',
    transactions='stg_stitch_sfmc_audience_transaction_with_first_gift_cohort'
) %}


with rev_by_cohort as (
select 
join_month_year_str,
first_gift_join_source,
join_gift_size_string_recur, 
first_gift_donor_audience,
month_diff_int,
concat('Ret' || month_diff_str ) as retention_str,
sum(amounts) as total_amount,
from {{ref(transactions)}} 
where first_gift_recur_status = 'one_time'
group by 1, 2, 3, 4, 5, 6

)

, add_cumulative as (
select 
join_month_year_str,
first_gift_join_source,
join_gift_size_string_recur, 
first_gift_donor_audience,
retention_str,
month_diff_int,
total_amount,
SUM(total_amount) OVER (
    PARTITION BY join_month_year_str, first_gift_join_source, join_gift_size_string_recur, first_gift_donor_audience, retention_str
    ORDER BY month_diff_int
  ) AS cumulative_amount
  from rev_by_cohort

)

, donors_in_cohort as (
select 
add_cumulative.*,
donors_in_cohort.donors_in_cohort,
from add_cumulative
left join {{ref(donors_in_cohort) }} donors_in_cohort
on add_cumulative.join_month_year_str = donors_in_cohort.join_month_year_str
and add_cumulative.first_gift_join_source = donors_in_cohort.first_gift_join_source
and add_cumulative.join_gift_size_string_recur = donors_in_cohort.join_gift_size_strin_recur
and add_cumulative.first_gift_donor_audience = donors_in_cohort.first_gift_donor_audience


)

select
join_month_year_str as join_month_year,
first_gift_join_source as join_source,
join_gift_size_string_recur as join_gift_size,
first_gift_donor_audience as join_donor_audience,
retention_str,
total_amount,
cumulative_amount,
donors_in_cohort
from donors_in_cohort

{% endmacro %}


