{% macro create_stg_stitch_sfmc_arc_person(
) %}

SELECT
    subscriberkey as person_id,
    date(cast(createddate as datetime)) as date_created,


    from {{ source("stitch_sfmc_arc", "arc_person") }}
 where person_type = 'Individual'


{% endmacro %}