with combined_data as (
    SELECT
    date_day,
    donor_audience,
    channel
  from {{ ref('stg_audience_channel_by_day_cross_join') }}
),

potential_explosions as (
  SELECT
    date_day,
    count(concat(donor_audience, channel)) as actual_combinations
  FROM combined_data 
  group by 1 
  having count(concat(donor_audience, channel)) > 5 * (select
  count(*)
from (
  select distinct donor_audience, channel from combined_data
)) as valid_combinations
)


select date_day, count(*) from potential_explosions group by 1 having count(*) > 0