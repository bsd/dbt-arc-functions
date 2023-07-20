{% macro create_stg_stitch_sfmc_audience_transactions_enriched_rollup(
    reference_name="stg_stitch_sfmc_parameterized_audience_transactions_enriched"
) %}

Select
    transaction_date_day,
    person_id,
    inbound_channel,
    recurring,
    (Case
        When recurring Is True Then gift_size_string
    End) As recurring_gift_size,
    sum(amount) As amounts,
    count(*) As gifts
from {{ ref(reference_name) }}
Group By 1, 2, 3, 4, 5


{% endmacro %}