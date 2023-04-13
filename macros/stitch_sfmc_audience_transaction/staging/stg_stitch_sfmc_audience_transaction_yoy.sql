{% macro create_stg_stitch_sfmc_audience_transaction_yoy(
    reference_name="stg_src_stitch_sfmc_transactions_unioned"
) %}

select transaction_date, person_id,
select
    transaction_date,
    person_id,
    max(
        case
            when
                date_trunc('year', transaction_date)
                = date_trunc('year', transaction_date)
            then 1
            else 0
        end
    ) as donated_this_year,
    max(
        case
            when
                date_trunc('year', dateadd('year', -1, transaction_date))
                = date_trunc('year', dateadd('year', -1, transaction_date))
            then 1
            else 0
        end
    ) as donated_last_year,
    max(
        case
            when
                date_trunc('year', dateadd('year', -2, transaction_date))
                = date_trunc('year', dateadd('year', -2, transaction_date))
            then 1
            else 0
        end
    ) as donated_two_years_ago,
    max(
        case
            when
                date_trunc('year', dateadd('year', -3, transaction_date))
                = date_trunc('year', dateadd('year', -3, transaction_date))
            then 1
            else 0
        end
    ) as donated_three_years_ago,
    max(
        case
            when
                transaction_date >= date_trunc('year', transaction_date)
                and person_id not in (
                    select distinct person_id
                    from {{ ref(reference_name) }}
                    where
                        transaction_date
                        >= date_trunc('year', dateadd('year', -1, transaction_date))
                        and transaction_date < date_trunc('year', transaction_date)
                )
            then 1
            else 0
        end
    ) as new_donor,
    max(
        case
            when transaction_date >= dateadd('month', -14, transaction_date)
            then 1
            else 0
        end
    ) as donated_within_14_months
    max(
        case
            when transaction_date >= dateadd('month', -13, transaction_date)
            then 1
            else 0
        end
    ) as donated_within_13_months
from {{ ref(reference_name) }}
group by transaction_date, person_id

{% endmacro %}
