{% macro create_stg_stitch_sfmc_src_donor_loyalty_validation(
    audience_union_transaction_joined="stg_stitch_sfmc_arc_audience_union_transaction_joined",
    donor_loyalty_count="stg_stitch_sfmc_arc_donor_loyalty_fiscal_year_count"
) %}

SELECT
    person_id_subquery.person_id,
    fiscal_year_subquery.fiscal_year,
    donor_loyalty_counts.start_date,
    donor_loyalty_counts.end_date,
    donor_loyalty_counts.row_num
FROM
    (SELECT DISTINCT person_id FROM {{ ref(audience_union_transaction_joined) }}) person_id_subquery
CROSS JOIN
    (SELECT DISTINCT fiscal_year FROM {{ ref(audience_union_transaction_joined) }}) fiscal_year_subquery
LEFT JOIN
    {{ ref(donor_loyalty_count) }} donor_loyalty_counts
ON
    person_id_subquery.person_id = donor_loyalty_counts.person_id AND f.fiscal_year = donor_loyalty_counts.fiscal_year
ORDER BY
    person_id_subquery.person_id,
    fiscal_year_subquery.fiscal_year


{% endmacro %}