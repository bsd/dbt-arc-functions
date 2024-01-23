{% macro create_stg_frakture_everyaction_deliverability_message_stat_unioned_with_domain(
    deliverability_stat="stg_frakture_everyaction_deliverability_message_stat_unioned",
    person="stg_frakture_everyaction_person_table_unioned"
) %}
select deliverability_stat.*, person.email_domain
from {{ ref(deliverability_stat) }} deliverability_stat
left join
    {{ ref(person) }} person
    on deliverability_stat.remote_person_id = person.remote_person_id
{% endmacro %}
