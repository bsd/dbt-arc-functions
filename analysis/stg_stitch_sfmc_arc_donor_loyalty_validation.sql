{% macro create_stg_stitch_sfmc_arc_donor_loyalty_validation(
    audience_union_transaction_joined="stg_stitch_sfmc_arc_audience_union_transaction_joined",
    donor_loyalty_count="stg_stitch_sfmc_arc_donor_loyalty_fiscal_year_count"
) %}

    select
        person_id_subquery.person_id,
        fiscal_year_subquery.fiscal_year,
        donor_loyalty_counts.start_date,
        donor_loyalty_counts.end_date,
        donor_loyalty_counts.row_num
    from
        (
            select distinct person_id from {{ ref(audience_union_transaction_joined) }}
        ) person_id_subquery
    cross join
        (
            select distinct fiscal_year
            from {{ ref(audience_union_transaction_joined) }}
        ) fiscal_year_subquery
    left join
        {{ ref(donor_loyalty_count) }} donor_loyalty_counts
        on person_id_subquery.person_id = donor_loyalty_counts.person_id
        and fiscal_year_subquery.fiscal_year = donor_loyalty_counts.fiscal_year
    order by person_id_subquery.person_id, fiscal_year_subquery.fiscal_year

{% endmacro %}
