with base as (
    select
    date_day,
    donor_audience,
    channel
  from {{ ref('stg_audience_channel_by_day_cross_join') }}
)

  select
    date_day,
    count(concat(donor_audience, channel)) as actual_combinations
  FROM base
  group by 1 
  having count(concat(donor_audience, channel)) > 5 * (select
  count(*)
from (
  select distinct donor_audience, channel from combined_data
)) as valid_combinations

