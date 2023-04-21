{% macro create_stg_stitch_sfmc_audience_transaction_yoy(
    reference_name="stg_stitch_sfmc_transactions_unioned"
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
                transaction_date
                >= date_trunc('year', dateadd('month', 6, current_date))
                and transaction_date < date_trunc(
                    'year', dateadd('year', 1, dateadd('month', 6, current_date))
                )
            then 1
            else 0
        end
    ) as donated_current_fiscal_year_july_to_june,
    max(
        case
            when
                transaction_date >= date_trunc(
                    'year', dateadd('month', 6, dateadd('year', -1, current_date))
                )
                and transaction_date
                < date_trunc('year', dateadd('month', 6, current_date))
            then 1
            else 0
        end
    ) as donated_last_fiscal_year_july_to_june,
    max(
        case
            when
                transaction_date >= date_trunc(
                    'year', dateadd('month', 6, dateadd('year', -2, current_date))
                )
                and transaction_date < date_trunc(
                    'year', dateadd('month', 6, dateadd('year', -1, current_date))
                )
            then 1
            else 0
        end
    ) as donated_two_fiscal_years_ago_july_to_june,
    max(
        case
            when
                transaction_date >= date_trunc(
                    'year', dateadd('month', 6, dateadd('year', -3, current_date))
                )
                and transaction_date < date_trunc(
                    'year', dateadd('month', 6, dateadd('year', -2, current_date))
                )
            then 1
            else 0
        end
    ) as donated_three_fiscal_years_ago_july_to_june,
    max(
        case
            when
                transaction_date >= date_trunc(
                    'year', dateadd('month', 6, dateadd('year', -4, current_date))
                )
                and transaction_date < date_trunc(
                    'year', dateadd('month', 6, dateadd('year', -4, current_date))
                )
            then 1
            else 0
        end
    ) as donated_four_fiscal_years_ago_july_to_june,
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
