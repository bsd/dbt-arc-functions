{% macro create_stg_stitch_sfmc_audience_transaction_cumulative(
    reference_name="stg_stitch_sfmc_transactions_unioned"
) %}

select
    transaction_date,
    person_id,
    -- total amount and gifts
    sum(amount) over (
        partition by person_id
        order by
            unix_timestamp(transaction_date) range
            between 2592000 preceding and current row
    ) as cumulative_amount_30_days,
    count(*) over (
        partition by person_id
        order by
            unix_timestamp(transaction_date) range
            between 2592000 preceding and current row
    ) as cumulative_gifts_30_days,
    sum(amount) over (
        partition by person_id
        order by
            unix_timestamp(transaction_date) range
            between 15778476 preceding and current row
    ) as cumulative_amount_6_months,
    count(*) over (
        partition by person_id
        order by
            unix_timestamp(transaction_date) range
            between 15778476 preceding and current row
    ) as cumulative_gifts_6_months,
    sum(amount) over (
        partition by person_id
        order by
            unix_timestamp(transaction_date) range
            between 31556952 preceding and current row
    ) as cumulative_amount_12_months,
    count(*) over (
        partition by person_id
        order by
            unix_timestamp(transaction_date) range
            between 31556952 preceding and current row
    ) as cumulative_gifts_12_months,
    sum(amount) over (
        partition by person_id
        order by
            unix_timestamp(transaction_date) range
            between 63113904 preceding and current row
    ) as cumulative_amount_24_months,
    count(*) over (
        partition by person_id
        order by
            unix_timestamp(transaction_date) range
            between 63113904 preceding and current row
    ) as cumulative_gifts_24_months,
    -- only recurring gifts
    -- total amount and gifts
    case
        when recurring = 1
        then
            sum(amount) over (
                partition by person_id
                order by
                    unix_timestamp(transaction_date) range
                    between 2592000 preceding and current row
            )
        else 0
    end as cumulative_amount_30_days_recur,
    case
        when recurring = 1
        then
            count(*) over (
                partition by person_id
                order by
                    unix_timestamp(transaction_date) range
                    between 2592000 preceding and current row
            )
        else 0
    end as cumulative_gifts_30_days_recur,
    case
        when recurring = 1
        then
            sum(amount) over (
                partition by person_id
                order by
                    unix_timestamp(transaction_date) range
                    between 15778476 preceding and current row
            )
        else 0
    end as cumulative_amount_6_months_recur,
    -- recurring gifts
    case
        when recurring = 1
        then
            count(*) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 15778800 preceding and current row
            )
        else 0
    end as cumulative_gifts_6_months_recur,
    case
        when recurring = 1
        then
            sum(amount) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 31557600 preceding and current row
            )
        else 0
    end as cumulative_amount_12_months_recur,
    case
        when recurring = 1
        then
            count(*) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 31557600 preceding and current row
            )
        else 0
    end as cumulative_gifts_12_months_recur,
    case
        when recurring = 1
        then
            sum(amount) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 63115200 preceding and current row
            )
        else 0
    end as cumulative_amount_24_months_recur,
    case
        when recurring = 1
        then
            count(*) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 63115200 preceding and current row
            )
        else 0
    end as cumulative_gifts_24_months_recur,
    -- one-time gifts
    case
        when recurring = 0
        then
            sum(amount) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 2592000 preceding and current row
            )
        else 0
    end as cumulative_amount_30_days_onetime,
    case
        when recurring = 0
        then
            count(*) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 2592000 preceding and current row
            )
        else 0
    end as cumulative_gifts_30_days_onetime,
    case
        when recurring = 0
        then
            sum(amount) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 15778800 preceding and current row
            )
        else 0
    end as cumulative_amount_6_months_onetime,
    case
        when recurring = 0
        then
            count(*) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 15778800 preceding and current row
            )
        else 0
    end as cumulative_gifts_6_months_onetime,
    case
        when recurring = 0
        then
            sum(amount) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 31557600 preceding and current row
            )
        else 0
    end as cumulative_amount_12_months_onetime,
    case
        when recurring = 0
        then
            count(*) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 31557600 preceding and current row
            )
        else 0
    end as cumulative_gifts_12_months_onetime,
    case
        when recurring = 0
        then
            sum(amount) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 63115200 preceding and current row
            )
        else 0
    end as cumulative_amount_24_months_onetime,
    case
        when recurring = 0
        then
            count(*) over (
                partition by person_id
                order by
                    transaction_date_unix
                    range between 63115200 preceding and current row
            )
        else 0
    end as cumulative_gifts_24_months_onetime

from {{ ref(reference_name) }}

{% endmacro %}
