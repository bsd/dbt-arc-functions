Select 
id as message_id,
date as date,
sum(impressions) as impressions
from {{ source("src_stitch_googleads_paidmedia", "google_ad_performance" ) }}
group by 1,2 