with combined_data as (
    SELECT
    date_day,
    donor_audience,
    channel
  from {{ ref('stg_audience_channel_by_day_cross_join') }}
),

distinct_data as (
select
  count(*) as valid_combinations
from (
  select distinct donor_audience, channel from combined_data
)
),

actual_data as (
  select
  date_day,
  count(donor_audience) as donor_audiences,
  count(channel) as channels,
  from combined_data
  group by 1
),



potential_explosions as (
  SELECT
    actual_data.date_day,
    actual_data.donor_audiences,
    actual_data.channels,
    valid_combinations
  FROM actual_data
  CROSS JOIN distinct_data
  WHERE (donor_audiences * channels) > valid_combinations
)

select date_day, count(*) from potential_explosions group by 1 having count(*) > 0