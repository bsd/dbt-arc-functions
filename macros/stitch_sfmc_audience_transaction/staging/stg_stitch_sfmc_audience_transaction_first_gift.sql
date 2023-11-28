{% macro create_stg_stitch_sfmc_audience_transaction_first_gift(
    audience="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched"
) %}

    {{
        config(
            materialized="table",
            partition_by={
                "field": "first_transaction_date",
                "data_type": "date",
                "granularity": "day",
            },
            cluster_by=["first_gift_recur_status"],
        )
    }}

    /*

This macro finds the first gift that a person gave, 
and pulls attributes from that first gift. 

*/
  WITH dedupe AS (
    SELECT
        person_id,
        transaction_id,
        transaction_date_day
    FROM (
        SELECT
            person_id,
            transaction_id,
            transaction_date_day,
            ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY transaction_date_day) as rn
        FROM {{ ref(audience) }}
    ) subquery
    WHERE rn = 1
)
 , first_transactions as(

SELECT
    person_id,
    transaction_id,
    transaction_date_day as first_transaction_date
FROM dedupe

 )

    select
        first_transactions.person_id,
        audience.transaction_id,
        first_transactions.first_transaction_date,
        cast(
            timestamp_trunc(first_transactions.first_transaction_date, day) as date
        ) as join_month_year_date,
        format_timestamp(
            '%b %Y', timestamp_trunc(first_transactions.first_transaction_date, month)
        ) as join_month_year_str,
        audience.channel as first_gift_join_source,
        (
            case
                when audience.amount between 0 and 25.99
                then '0-25'
                when audience.amount between 26 and 100.99
                then '26-100'
                when audience.amount between 101 and 250.99
                then '101-250'
                when audience.amount between 251 and 500.99
                then '251-500'
                when audience.amount between 501 and 1000.99
                then '501-1000'
                when audience.amount between 1001 and 10000.99
                then '1001-10000'
                when audience.amount > 10000
                then '10000+'
            end
        ) as join_gift_size_string,
        (
            case
                when audience.amount between 0 and 10.99
                then '0-10'
                when audience.amount between 11 and 20.99
                then '11-20'
                when audience.amount between 21 and 30.99
                then '21-30'
                when audience.amount between 31 and 40.99
                then '31-40'
                when audience.amount between 41 and 50.99
                then '41-50'
                when audience.amount between 51 and 60.99
                then '51-60'
                when audience.amount between 61 and 70.99
                then '61-70'
                when audience.amount between 71 and 80.99
                then '71-80'
                when audience.amount between 81 and 90.99
                then '81-90'
                when audience.amount between 91 and 100.99
                then '91-100'
                when audience.amount > 100
                then '100+'
            end
        ) as join_gift_size_string_recur,
        audience.recurring as first_gift_recur_status,
        audience.donor_audience as first_gift_donor_audience,
        audience.amount
    from first_transactions
    left join
    {{ ref(audience) }} audience
    on first_transactions.transaction_id = audience.transaction_id
{% endmacro %}
