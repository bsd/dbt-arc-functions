{% macro create_stg_sfmc_audience_transaction_onetime_first_gift_by_cohort() %}

{{dbt_arc_functions.util_stg_stitch_audience_first_gift_by_cohort(recur_status='recurring')}}


        {% endmacro %}

