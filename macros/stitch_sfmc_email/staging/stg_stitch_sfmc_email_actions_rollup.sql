{% macro create_stg_stitch_sfmc_email_actions_rollup(
    reference_name="stg_stitch_sfmc_email_actions"
) %}
select distinct message_id, 
SUM(actions) as actions
from {{ ref(reference_name) }}
group by 1
{% endmacro %}
