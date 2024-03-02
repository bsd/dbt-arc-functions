{{ config(severity="warn") }}
select 
first_gift_donor_audience, count(distinct person_id) as people 
from {{ref("stg_stitch_sfmc_parameterized_audience_transaction_first_gift")}}
where first_gift_donor_audience not like '%Prospect%'
group by 1