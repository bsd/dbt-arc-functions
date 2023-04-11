{% macro create_stg_stitch_sfmc_audience_transaction_yoy(
    reference_name="stg_src_stitch_sfmc_transactions_unioned"
) %}

select
    person_id,
    max(
        case
            when date_diff(current_date(), transaction_date, year) = 0 then 1 else 0
        end
    ) as donated_this_year,
    max(
        case
            when date_diff(current_date(), transaction_date, year) = 1 then 1 else 0
        end
    ) as donated_last_year,
    max(
        case
            when date_diff(current_date(), transaction_date, year) = 2 then 1 else 0
        end
    ) as donated_two_years_ago,
    max(
        case
            when date_diff(current_date(), transaction_date, year) = 3 then 1 else 0
        end
    ) as donated_three_years_ago

from {{ ref(reference_name) }}
group by person_id

{% endmacro %}
