{% test assert_first_donation_ever(model, frequency) %}

{{ config(severity="warn") }}

with first_donations_ever as (
  select person_id, min(date_day) as first_donation_date
  from  {{ref('stg_audience_donors_by_day')}}
  where recurring = {% if frequency == 'recurring'%} true {% else %} false {% endif %}
  group by person_id
),
potential_first_donations as (
  select date_day, donor_audience, channel, person_id
  from {{model}}
  where 
  {% if frequency == 'recurring' %} new_recur_donor_counts > 0 {% else %} new_onetime_donor_counts > 0 {% endif %}
),

incorrect_first_donations as (
  select 
    potential_first_donations.date_day,
    potential_first_donations.donor_audience,
    potential_first_donations.channel,
    potential_first_donations.person_id
  from potential_first_donations
  left join first_donations_ever
    on potential_first_donations.person_id = first_donations_ever.person_id 
    and potential_first_donations.date_day = first_donations_ever.first_donation_date
  where first_donations_ever.person_id is null  -- Not the person's actual first donation
),

counts as (
  select count(*) as errors from incorrect_first_donations
)

select * from counts
where errors > 0

{% endtest %}