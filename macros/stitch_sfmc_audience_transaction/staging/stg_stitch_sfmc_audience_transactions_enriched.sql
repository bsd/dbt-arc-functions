{% macro create_stg_stitch_sfmc_audience_transactions_enriched(
    reference_name="stg_stitch_sfmc_audience_transactions_summary_unioned"
) %}

Select
transaction_id,
person_id,
transaction_date_day,
amount,
inbound_channel,
(case
        when (lower(source_code) like 'email_%' or lower(source_code) like '%bsd%') then 'Email'
        when lower(source_code) like 'web%' then 'Web'
        when (lower(source_code) like 'cpc_%' or lower(source_code) like 'sem_%') then 'CPC'
        when (lower(source_code) like 'ref_%' or lower(source_code) like 'referral_%') then 'Referral'
        when lower(source_code) like 'organic_%' then 'Organic'
        when lower(source_code) like 'paidsocial_%' then 'Paid Social'
        when lower(source_code) like 'display%' then 'Display'
        when lower(source_code) like 'social%' then 'Social'
        when source_code is null then 'No Source'
        else inbound_channel
      end) as best_guess_inbound_channel, -- This is custom logic for UUSA.
recurring,
(case
           when amount BETWEEN 0 AND 25 then '0-25'
           when amount BETWEEN 26 AND 100 then '26-100'
           when amount BETWEEN 101 AND 250 then '101-250'
           when amount BETWEEN 251 AND 500 then '251-500'
           when amount BETWEEN 501 AND 1000 then '501-1000'
           when amount BETWEEN 1001 AND 10000 then '1001-10000'
           else '10000+'
       end) as gift_size_string,
ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY transaction_date_day) AS gift_count
from {{ ref(reference_name) }}


{% endmacro %}







