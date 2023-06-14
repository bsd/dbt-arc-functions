{% macro create_stg_stitch_sfmc_arc_audience_date_spine() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^arc_person$",
        is_source=True,
        source_name="src_stitch_sfmc_arc",
        schema_to_search="src_stitch_sfmc_arc_authorized",
    ) %}

select date
from
    unnest(
        generate_date_array(
            (select min(date(dbt_valid_from)) from {{ ref("person_snapshot") }}),
            ifnull(
                (select max(date(dbt_valid_to)) from {{ ref("person_snapshot") }}),
                current_date()
            )
        )
    ) as date
