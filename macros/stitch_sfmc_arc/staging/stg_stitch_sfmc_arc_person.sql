{% macro create_stg_stitch_sfmc_arc_person(
    first_gift_transactions="stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned"
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
            select
                person_id,
                transaction_id,
                transaction_date_day as first_transaction_date
            from dedupe
        ),
        person as (
            select
                subscriberkey as person_id,
                date(cast(createddate as datetime)) as date_created
            from {{ source("src_stitch_sfmc_arc", "arc_person") }}
            {% if target.name != "prod" %}
                where
                    cast(createddate as datetime)
                    >= date_sub(current_date(), interval 5 year)
            {% endif %}
        )

    select
        coalesce(person.person_id, first_transactions.person_id) as person_id,
        person.date_created,
        first_transactions.first_transaction_date
    from person
    full join first_transactions using (person_id)

{% endmacro %}
