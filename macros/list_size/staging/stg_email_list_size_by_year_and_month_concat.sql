{% macro create_stg_email_list_size_year_and_month_concat(
    reference_name='stg_email_list_size_by_year_and_month'
) %}
SELECT
CONCAT(extract_year,'-',extract_month,'-01') as concat_date,
max_recipients,
max_delivered
FROM  {{ ref(reference_name) }} mart_email

{% endmacro %}


