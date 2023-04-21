{% macro create_stg_stitch_sfmc_audience_transaction_yoy(
    reference_name="stg_stitch_sfmc_transactions_unioned"
) %}

with
    fiscal_years as (
        select
            date_trunc(
                date_add(transaction_date, interval 6 month), year
            ) as fiscal_year_start,
            date_trunc(
                date_add(transaction_date, interval 18 month), year
            ) as fiscal_year_end,
            *
        from {{ ref(reference_name) }}
    )

select
    transaction_date,
    person_id,
    1 as donated_current_fiscal_year_july_to_june,
    max(
        case
            when
                transaction_date >= date_trunc(
                    date_add(fiscal_years.fiscal_year_start, interval - 1 year), year
                )
                and transaction_date < fiscal_years.fiscal_year_start
            then 1
            else 0
        end
    ) as donated_last_fiscal_year_july_to_june,
    max(
        case
            when
                transaction_date >= date_trunc(
                    date_add(fiscal_years.fiscal_year_start, interval - 2 year), year
                )
                and transaction_date < date_trunc(
                    date_add(fiscal_years.fiscal_year_start, interval - 1 year), year
                )
            then 1
            else 0
        end
    ) as donated_two_fiscal_years_ago_july_to_june,
    max(
        case
            when
                transaction_date >= date_trunc(
                    date_add(fiscal_years.fiscal_year_start, interval - 3 year), year
                )
                and transaction_date < date_trunc(
                    date_add(fiscal_years.fiscal_year_start, interval - 2 year), year
                )
            then 1
            else 0
        end
    ) as donated_three_fiscal_years_ago_july_to_june,
    max(
        case
            when
                transaction_date >= date_trunc(
                    date_add(fiscal_years.fiscal_year_start, interval - 4 year), year
                )
                and transaction_date < date_trunc(
                    date_add(fiscal_years.fiscal_year_start, interval - 3 year), year
                )
            then 1
            else 0
        end
    ) as donated_four_fiscal_years_ago_july_to_june,
    max(
        case
            when
                transaction_date
                >= date_add(date_trunc(transaction_date, month), interval - 14 month)
            then 1
            else 0
        end
    ) as donated_within_14_months,
    max(
        case
            when
                transaction_date
                >= date_add(date_trunc(transaction_date, month), interval - 13 month)
            then 1
            else 0
        end
    ) as donated_within_13_months
from fiscal_years
group by 1, 2

{% endmacro %}
