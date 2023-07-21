{% macro create_stg_adhoc_google_spreadsheets_audience_monthly_budget_date_spine(
    reference_name='stg_adhoc_google_spreadsheets_audience_monthly_budget') %}
{% set min_date_query %}
SELECT min(start_date) FROM {{ ref(reference_name) }}
{% endset %}
{% set min_date_results = run_query(min_date_query) %}
{% if execute %}
    {% set min_date %}'{{min_date_results.columns[0].values()[0]}}'{% endset %}
{% else %} {% set min_date = "2020-01-01" %}
{% endif %}

{% set max_date_query %}
SELECT max(end_date) FROM {{ ref(reference_name) }}
{% endset %}
{% set max_date_results = run_query(max_date_query) %}
{% if execute %}
    {% set max_date %}'{{max_date_results.columns[0].values()[0]}}'{% endset %}
{% else %} {% set max_date = "2020-01-01" %}
{% endif %}

{{ dbt_utils.date_spine(datepart="day", start_date=min_date, end_date=max_date) }}

{% endmacro %}