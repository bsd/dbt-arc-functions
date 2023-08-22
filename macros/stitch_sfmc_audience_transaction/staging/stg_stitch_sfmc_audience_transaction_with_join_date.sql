{% macro create_stg_stitch_sfmc_audience_transaction_with_join_date(
    reference_0_name='stg_stitch_sfmc_arc_audience_union_transaction_joined',
    reference_1_name='stg_stitch_sfmc_audience_transaction_first_gift') %}
SELECT
    union_transaction.transaction_date_day AS transaction_date_day,
    union_transaction.person_id AS person_id,
    first_gift.join_month_year_date AS join_month_year_date,
    union_transaction.amount AS amount
FROM
    {{ ref(reference_0_name) }}
        AS union_transaction
INNER JOIN
    {{ ref(reference_1_name) }} AS first_gift
    ON union_transaction.person_id = first_gift.person_id
WHERE
    union_transaction.recurring = True
{% endmacro %}