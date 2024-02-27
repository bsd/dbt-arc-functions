select distinct join_donor_audience
from {{ ref("mart_audience_1x_activation") }}
/* insert variations of monthly audience names that just shouldn't show up here at all */
where lower(join_donor_audience) = 'monthly' or lower(join_donor_audience) = 'recurring'
