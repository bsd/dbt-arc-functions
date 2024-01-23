{% macro create_stg_audience_transaction_onetime_rev_by_cohort() %}

    {{
        dbt_arc_functions.util_stg_audience_transaction_rev_by_cohort(
            recur_status="onetime"
        )
    }}

{% endmacro %}
