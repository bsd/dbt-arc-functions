{% macro create_stg_audience_budget_onetime_donor_count_daily() %}
   
   {{ dbt_arc_functions.util_stg_audience_budget_frequency_donor_counts_interval_type(
        interval_type='day',
        frequency='onetime'
   )}}

{% endmacro %}
