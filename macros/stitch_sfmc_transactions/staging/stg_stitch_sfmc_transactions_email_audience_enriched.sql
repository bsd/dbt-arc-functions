{% macro create_stg_stitch_sfmc_transactions_email_audience_enriched(
    reference_name="stg_stitch_sfmc_transactions_unioned",
    job = 'stg_src_stitch_email_job'

) %}

Select u.*, e.email_name,
(case when (lower(e.email_name) like '%active%' and lower(e.email_name) not like '%inactive%') or lower(e.email_name) like '%mass%' then 'Mass'
           when lower(e.email_name) like '%inactive%' then 'Inactive'
           when lower(e.email_name) like '%mid%level%' or lower(e.email_name) like '%leadership%giving%' then 'Leadership Giving'
           when lower(e.email_name) like '%monthly%' then 'Monthly'
           when lower(e.email_name) like '% other %' then 'Other - Targeted'
           when lower(e.email_name) like '%ramp%' then 'IP Ramp'
           when lower(e.email_name) like '%lapse%' then 'Lapsed'
           when lower(e.email_name) like '%major%donor%' then 'Major Donors'
           else 'Other'
           end) as audience_name
from {{ ref(reference_name) }} u
inner join {{ ref(job) }} e on CAST(u.message_id as String) = e.message_id



{% endmacro %}
