{% macro create_mart_email_list_size_by_month_date(
    reference_name='stg_email_list_size_by_year_and_month_concat'
) %}
SELECT
SAFE_CAST(concat_date AS DATE) AS date_month,
SAFE_CAST(max_recipients AS INT) AS max_recipients,
SAFE_CAST(max_delivered as INT) AS max_delivered
FROM  {{ ref(reference_name) }} mart_email

{% endmacro %}


