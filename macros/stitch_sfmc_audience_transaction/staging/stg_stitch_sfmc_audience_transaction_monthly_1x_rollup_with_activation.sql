{% macro create_stg_stitch_sfmc_audience_transaction_monthly_1x_rollup_with_activation(
    reference_name="stg_stitch_sfmc_audience_transaction_monthly_1x_rollup"
) %}
select
    join_month_year,
    transaction_month_year_date,
    donor_audience,
    channel,
    join_gift_size_string,
    concat(
        cast(extract(year from join_month_year) as string),
        '-',
        lpad(cast(extract(month from join_month_year) as string), 2, '0')
    ) as join_month_year_str,
    concat(
        cast(extract(year from transaction_month_year_date) as string),
        '-',
        lpad(cast(extract(month from transaction_month_year_date) as string), 2, '0')
    ) as transaction_month_year_str,
    concat(
        'Act',
        lpad(
            cast(
                date_diff(transaction_month_year_date, join_month_year, month) as string
            ),
            2,
            '0'
        )
    ) as activation,
    total_revenue,
    total_donors
from {{ ref(reference_name) }}

{% endmacro %}
