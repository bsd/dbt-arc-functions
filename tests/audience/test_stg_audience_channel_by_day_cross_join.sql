{{ config(severity="error", warn_if=">1", error_if=">10") }}


with combined_data as (
    SELECT
    date_day,
    donor_audience,
    channel
  from {{ ref('stg_audience_channel_by_day_cross_join') }}
)

select
  count(*) as total_rows,
  count(distinct donor_audience) as distinct_audiences,
  count(distinct channel) as distinct_channels
from combined_data
-- Check for significant difference between total rows and unique audience/channel counts
having
  count(*) > 10 * (count(distinct donor_audience) + count(distinct channel))
