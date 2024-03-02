{{ config(severity="warn") }}
select 
distinct first_gift_donor_audience 
from {{ref("stg_stitch_sfmc_parameterized_audience_transaction_first_gift")}}
where first_gift_donor_audience not like '%Prospect%'