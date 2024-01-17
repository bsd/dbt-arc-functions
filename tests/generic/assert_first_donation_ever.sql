{% test assert_first_donation_ever(model, frequency) %}

{{ config(severity="warn") }}

with first_donations_ever as (
  select first_transaction_date,
          count(distinct person_id) as actual_new_donors
  from  prod_staging.stg_audience_parameterized_transaction_first_gift
  where first_gift_recur_status = {% if frequency == 'recurring'%} true {% else %} false {% endif %}
  group by 1
),

potential_first_donations as (
  select date_day,
  {% if frequency == 'recurring' %} new_recur_donor_counts {% else %} new_onetime_donor_counts {% endif %} as potential_new_donors
  from {{model}}
  
),

incorrect_first_donations as (
  select 
    potential_first_donations.date_day,
    potential_first_donations.potential_new_donors
  from potential_first_donations
  left join first_donations_ever on
   potential_first_donations.date_day = first_donations_ever.first_transaction_date
   where potential_first_donations.potential_new_donors != first_donations_ever.actual_new_donors
  
),

counts as (
  select count(*) as errors from incorrect_first_donations
)

select * from counts
where errors > 0

{% endtest %}