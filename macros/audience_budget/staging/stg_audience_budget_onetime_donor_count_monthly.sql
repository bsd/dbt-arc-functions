{% macro create_stg_audience_budget_onetime_donor_count_monthly() %}
    
    {{ dbt_arc_functions.util_stg_audience_budget_frequency_donor_counts_interval_type(
        interval_type='month',
        frequency='onetime'
   )}}

{% endmacro %}
