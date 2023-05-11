{% macro create_stg_frakture_sfmc_person_message_stat_unioned_with_domain(
    person_stat="stg_frakture_sfmc_person_message_stat_unioned",
    person="stg_frakture_sfmc_person_table_unioned"
) %}
    select person_stat.*, person.email_domain
    from {{ ref(person_stat) }} person_stat
    left join
        {{ ref(person) }} person
        on person_stat.remote_person_id = person.remote_person_id
{% endmacro %}
