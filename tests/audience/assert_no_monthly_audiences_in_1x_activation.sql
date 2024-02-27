select distinct donor_audience from {{ref('mart_audience_1x_activation')}}
/* insert variations of monthly audience names that just shouldn't show up here at all */
where lower(donor_audience) = 'monthly'
or lower(donor_audience) = 'recurring'