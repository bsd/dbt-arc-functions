select 
distinct first_gift_donor_audience 
from {{ref("stitch_sfmc_parameterized_audience_transaction_first_gift")}}
where lower(first_gift_donor_audience) != 'prospect new'
or lower(first_gift_donor_audience) != 'prospect existing'