{% macro create_stg_stitch_sfmc_email_campaigns(
    reference_name='stg_src_stitch_email_job') %}

SELECT DISTINCT
  SAFE_CAST(job_id AS STRING) AS message_id,
  SAFE_CAST('sfmc' as STRING) as crm_entity,
  SAFE_CAST(null as STRING) as source_code_entity,
  SAFE_CAST((case
           when (lower(email_name) like '%active%' and lower(email_name) not like '%inactive%') or lower(email_name) like '%mass%' then 'Mass'
           when lower(email_name) like '%inactive%' then 'Inactive'
           when lower(email_name) like '%mid%level%' or lower(email_name) like '%leadership%giving%' then 'Leadership Giving'
           when lower(email_name) like '%monthly%' then 'Monthly'
           when lower(email_name) like '% other %' then 'Other - Targeted'
           when lower(email_name) like '%welcome%' then 'Welcome Series'
           when lower(email_name) like '%ramp%' then 'IP Ramp'
           when lower(email_name) like '%lapse%' then 'Lapsed'
           when lower(email_name) like '%major%donor%' then 'Major Donors'
           else 'Other'
           end) as STRING) as audience,
  SAFE_CAST(null as STRING) as recurtype,
  SAFE_CAST(null as STRING) as campaign_category,
  SAFE_CAST(null AS STRING) AS crm_campaign,
  SAFE_CAST(
    (case
        when lower(email_name) like '%postcard%' then 'Postcard'
        when lower(email_name) like '%newswire%' then 'Newswire'
        when regexp_contains(left(email_name, 1), r'^[0-9]$') then regexp_extract(email_name,r"([a-zA-Z]+)")
        when lower(email_name) like '%ukraine%donor%' then INITCAP(REGEXP_EXTRACT(lower(email_name), r'(.*?)ukraine donors.*'))
        when (lower(email_name) like '%mass%' or lower(email_name) like '%active%')
          then INITCAP(REGEXP_EXTRACT(lower(email_name), r'(.*?)mass.*'))
        when lower(email_name) like '%midlevel%' then INITCAP(REGEXP_EXTRACT(lower(email_name), r'(.*?)midlevel.*'))
        when lower(email_name) like '%leadership%giving%' then INITCAP(REGEXP_EXTRACT(lower(email_name), r'(.*?)leadership giving.*'))
        when lower(email_name) like '%monthly%' then INITCAP(REGEXP_EXTRACT(lower(email_name), r'(.*?)monthly.*'))
        when lower(email_name) like '%major%' then INITCAP(REGEXP_EXTRACT(lower(email_name), r'(.*?)major.*'))
        when lower(email_name) like '%unite%' then INITCAP(REGEXP_EXTRACT(lower(email_name), r'(.*?)unite.*'))
        when lower(email_name) like '%clubs%' then INITCAP(REGEXP_EXTRACT(lower(email_name), r'(.*?)clubs.*'))
        when (lower(email_name) like 'niche%' and lower(email_name) like '%series%') then 'Niche WS'
        when (lower(email_name) like 'advocacy%' and lower(email_name) like '%series%') then 'Advocacy WS'
        when lower(email_name) like 'new%name%' then 'New Name WS'
        when lower(email_name) like 'new%donor%'  then 'New Donor WS'
        when lower(email_name) like '%quiz%series%' then 'Quiz WS'
        when lower(email_name) like '%monthly%nurture%' then 'Monthly Nurture Series'
        when lower(email_name) like '%reactivation%' then 'Reactivation Series'
        when lower(email_name) like '%upsell%' then 'Upsell Series'
        when lower(email_name) not like '%series%' then regexp_extract(email_name,r"([a-zA-Z]+)")
        end) AS STRING) AS source_code_campaign

FROM {{ ref(reference_name) }}

{% endmacro %}