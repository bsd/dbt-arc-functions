{% macro create_stg_stitch_sfmc_arc_audience_date_spine() %}
    {% set relations = dbt_arc_functions.relations_that_match_regex(
        "^arc_audience$",
        is_source=True,
        source_name="src_stitch_sfmc_arc",
        schema_to_search="src_stitch_sfmc_arc_authorized",
    ) %}

select date
from
    unnest(
        generate_date_array(
            (select min(date(CAST(CONCAT(Substr(dbt_valid_from, 0, 22), " America/New_York") as timestamp), "America/New_York"))
            from {{ ref("arc_audience_snapshot") }}),


            ifnull(
                (select max(date(CAST(CONCAT(Substr(dbt_valid_to, 0, 22), " America/New_York") as timestamp), "America/New_York"))

                from {{ ref("arc_audience_snapshot") }}),
                current_date()
            )
        )
    ) as date