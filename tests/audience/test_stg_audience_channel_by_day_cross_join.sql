with combined_data as (
    SELECT
    date_day,
    donor_audience,
    channel
  from {{ ref('stg_audience_channel_by_day_cross_join') }}
),

distinct_data as (
select
  count(distinct date_day) as distinct_days,
  count(distinct donor_audience) as distinct_audiences,
  count(distinct channel) as distinct_channels,
from combined_data
),

actual_data as (
  select
  date_day,
  count(donor_audience) as donor_audiences,
  count(channel) as channels,
  from combined_data
  group by 1
),

mult_distinct_data as (
  select 
  distinct_audiences,
  distinct_channels,
  (distinct_audiences * distinct_channels) as valid_combinations
  from distinct_data
),

potential_explosions as (
  SELECT
    actual_data.date_day,
    actual_data.donor_audiences,
    actual_data.channels,
    valid_combinations
  FROM actual_data
  CROSS JOIN mult_distinct_data
  WHERE (donor_audiences * channels) > valid_combinations
)

select count(*) from potential_explosions having count(*) > 0