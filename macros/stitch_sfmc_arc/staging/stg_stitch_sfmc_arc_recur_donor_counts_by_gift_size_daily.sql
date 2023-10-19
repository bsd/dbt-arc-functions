{% macro create_stg_stitch_sfmc_arc_recur_donor_counts_by_gift_size_daily(
    audience_transaction= "stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched"

) %}


with base as (
Select
    transaction_date_day As date_day,
    channel,
    coalesced_audience as donor_audience,
    gift_size_string As gift_size,
    count(Distinct person_id) As donor_counts
From {{ ref(audience_transaction) }}
Where recurring = True
Group By 1, 2, 3, 4
Order By 1 Desc, 4
)
Select
'daily' As interval_type,
*
from base

{% endmacro %}
