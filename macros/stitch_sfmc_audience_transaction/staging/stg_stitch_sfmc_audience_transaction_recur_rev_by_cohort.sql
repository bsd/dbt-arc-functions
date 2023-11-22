{% macro create_stg_stitch_sfmc_audience_transaction_recur_rev_by_cohort() %}

    {{
        dbt_arc_functions.util_stg_stitch_sfmc_audience_transaction_rev_by_cohort(
            recur_status="recurring"
        )
    }}

{% endmacro %}
