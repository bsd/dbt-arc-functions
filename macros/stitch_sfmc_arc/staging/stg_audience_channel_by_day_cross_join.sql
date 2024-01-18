
{% macro create_stg_audience_channel_by_day_cross_join(
    person_and_transaction="stg_stitch_sfmc_audience_transactions_enriched_rollup_join_person_and_transaction"
) %}
 
select
    date_day,
    distinct_donor_audience.donor_audience,
    distinct_channel.channel
from {{ ref(person_and_transaction)}}
cross join (
    select distinct donor_audience from {{ ref(person_and_transaction) }}
) as distinct_donor_audience
cross join (
    select distinct channel from {{ ref(person_and_transaction) }}
) as distinct_channel

{% endmacro %}
