-- sqlfmt: disable=space-around-variables

{% macro create_stg_stitch_sfmc_arc_donor_loyalty_fiscal_year_count(
    audience_union_transaction_joined="stg_stitch_sfmc_arc_audience_union_transaction_joined"
) %}

    select
        person_id,
        fiscal_year,
        min(transaction_date_day) as start_date,
        date_sub(
            date(concat(fiscal_year, '-', '{{ var('fiscal_year_start') }}')),
            interval 1 day
        ) as end_date,

        row_number() over (partition by person_id order by fiscal_year desc) as row_num
    from {{ ref(audience_union_transaction_joined) }}
    group by person_id, fiscal_year
    order by person_id, fiscal_year

{% endmacro %}

-- sqlfmt: enable=space-around-variables