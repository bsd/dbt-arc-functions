{% macro create_stg_stitch_sfmc_arc_audience_by_date_day(
    date_spine = "stg_stitch_sfmc_arc_audience_date_spine",
    audience_snapshot = "snp_stitch_sfmc_arc_audience"
) %}

WITH audience_by_date_day as (
    select
    d.date as date_day,
    s.subscriberkey as person_id,
    s.__donoraudience_ as arc_audience
from {{ ref(date_spine) }} as d
inner join
    {{ ref(audience_snapshot) }} as s
    on
        d.date >= date(CAST(CONCAT(Substr(s.dbt_valid_from, 0, 22), " America/New_York") as timestamp), "America/New_York")
        and (d.date <= date(CAST(CONCAT(Substr(s.dbt_valid_to, 0, 22), " America/New_York") as timestamp), "America/New_York")
        or date(CAST(CONCAT(Substr(s.dbt_valid_to, 0, 22), " America/New_York") as timestamp), "America/New_York") is null)
), deduplicated_table as (
    SELECT
        date_day,
        person_id,
        arc_audience,
        ROW_NUMBER()
            OVER (PARTITION BY date_day, person_id ORDER BY arc_audience)
            AS row_num
    FROM audience_by_date_day

)

SELECT
    date_day,
    person_id,
    arc_audience
FROM deduplicated_table
WHERE row_num = 1



{% endmacro %}