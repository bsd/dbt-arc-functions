{% macro create_stg_stitch_sfmc_parameterized_audience_transactions_enriched(
    channel,
    reference_name="stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned"
) %}

    {{
        config(
            materialized="table",
            partition_by={
                "field": "transaction_date_day",
                "data_type": "date",
                "granularity": "day",
            },
            cluster_by=["recurring", "person_id"],
        )
    }}

    with
        base as (
            select
                transaction_id,
                person_id,
                transaction_date_day,
                cast(amount as float64) as amount,
                initcap({{ channel }}) as channel,
                appeal_business_unit,
                recurring,
                (
                    case
                        when amount between 0 and 25.99
                        then '0-25'
                        when amount between 26 and 100.99
                        then '26-100'
                        when amount between 101 and 250.99
                        then '101-250'
                        when amount between 251 and 500.99
                        then '251-500'
                        when amount between 501 and 1000.99
                        then '501-1000'
                        when amount between 1001 and 10000.99
                        then '1001-10000'
                        when amount > 10000
                        then '10000+'
                    end
                ) as gift_size_string,
                (
                    case
                        when amount between 0 and 10.99
                        then '0-10'
                        when amount between 11 and 20.99
                        then '11-20'
                        when amount between 21 and 30.99
                        then '21-30'
                        when amount between 31 and 40.99
                        then '31-40'
                        when amount between 41 and 50.99
                        then '41-50'
                        when amount between 51 and 60.99
                        then '51-60'
                        when amount between 61 and 70.99
                        then '61-70'
                        when amount between 71 and 80.99
                        then '71-80'
                        when amount between 81 and 90.99
                        then '81-90'
                        when amount between 91 and 100.99
                        then '91-100'
                        when amount > 100
                        then '100+'
                    end
                ) as gift_size_string_recur,
                row_number() over (
                    partition by transaction_id order by transaction_date_day
                ) as row_number
            from {{ ref(reference_name) }}

        )

    select
        *,
        row_number() over (
            partition by person_id order by transaction_date_day
        ) as gift_count
    from base
    where row_number = 1

{% endmacro %}
