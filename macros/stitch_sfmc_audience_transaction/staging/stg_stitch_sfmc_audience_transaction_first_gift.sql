{% macro create_stg_stitch_sfmc_audience_transactions_first_gift(
    reference_name="stg_stitch_sfmc_audience_transactions_enriched"
) %}

with first_transactions as (
Select
        person_id,
        transaction_date_day AS FirstTransactionDate,
        inbound_channel AS FirstTransactioninbound_channel,
        SAFE_CAST(amount as INT64) as FirstTransactionAmount,
        best_guest_inbound_channel,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY transaction_date_day asc) AS row_number
from
{{ ref(reference_name) }} ft
)
SELECT
    ft.person_id,
    cast(
        timestamp_trunc(ft.FirstTransactionDate, day) as date
        ) as join_month_year_date,
    ft.best_guess_inbound_channel as first_gift_join_source,
    ft.FirstTransactionAmount  as first_gift_amount_int,
    (case
           when ft.FirstTransactionAmount BETWEEN 0 AND 25 then '0-25'
           when ft.FirstTransactionAmount BETWEEN 26 AND 100 then '26-100'
           when ft.FirstTransactionAmount BETWEEN 101 AND 250 then '101-250'
           when ft.FirstTransactionAmount BETWEEN 251 AND 500 then '251-500'
           when ft.FirstTransactionAmount BETWEEN 501 AND 1000 then '501-1000'
           when ft.FirstTransactionAmount BETWEEN 1001 AND 10000 then '1001-10000'
           else '10000+'
       end) as join_gift_size_string
FROM
    first_transactions AS ft
WHERE ft.row_number = 1

{% endmacro %}