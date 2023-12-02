Select 
id as message_id,
date as date,
(cost_micros/1000000) as spend
from {{ source("src_stitch_googleads_paidmedia", "google_ad_performance" ) }}


