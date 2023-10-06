{% macro create_stg_stitch_sfmc_audience_transaction_yoy(
    reference_name="stg_stitch_sfmc_audience_transactions_summary_unioned"
) %}


    select
        transaction_date_day,
        person_id,
        max(
            case
                when
                    transaction_date >= date_add(
                        date_trunc(transaction_date_day, month), interval - 14 month
                    )
                then 1
                else 0
            end
        ) as donated_within_14_months,
        max(
            case
                when
                    transaction_date_day >= date_add(
                        date_trunc(transaction_date_day, month), interval - 13 month
                    )
                then 1
                else 0
            end
        ) as donated_within_13_months
    from {{ ref(reference_name)}}
    group by 1, 2

{% endmacro %}
