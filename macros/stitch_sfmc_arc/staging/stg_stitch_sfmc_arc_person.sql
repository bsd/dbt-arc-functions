{% macro create_stg_stitch_sfmc_arc_person(
first_gift_transactions = "stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned"
    ) %}

    with
        dedupe as (
            select person_id, transaction_id, transaction_date_day
            from
                (
                    select
                        person_id,
                        transaction_id,
                        transaction_date_day,
                        row_number() over (
                            partition by person_id order by transaction_date_day
                        ) as rn
                    from {{ ref(first_gift_transactions) }}
                ) subquery
            where rn = 1
        ),
        first_transactions as (
        select person_id,
        transaction_id,
        transaction_date_day as first_transaction_date
        from dedupe
        )

    SELECT
    person.subscriberkey as person_id,
    date(cast(person.createddate as datetime)) as date_created,
    first_transactions.first_transaction_date
    from {{ source("src_stitch_sfmc_arc", "arc_person") }} person
    left join first_transactions on person.subscriberkey = first_transactions.person_id
 where persontype = 'Individual'


{% endmacro %}