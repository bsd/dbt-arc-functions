{% macro create_stg_stitch_sfmc_audience_calculated_audience_by_date_day(
    audience_snapshot="snp_stitch_sfmc_arc_audience",
    calculated_audience="stg_stitch_sfmc_audience_parameterized_calculated_audience"
) %}


-- macros/stitch_sfmc_audience_transaction/staging/stg_stitch_sfmc_audience_transaction_calculated_date_spine.sql

with stg_stitch_sfmc_audience_transaction_calculated_date_spine as (

select date
    from
        unnest(
            generate_date_array(
                (
                    select min(transaction_date_day),
                    from {{ ref(calculated_audience) }}
                ),
                ifnull(
                    (
                        select min(date(cast(concat(
                                            substr(dbt_valid_from, 0, 22),
                                            " America/New_York"
                                        ) as timestamp
                                    ),
                                    "America/New_York"
                                ) - 1
                            )

                        from {{ ref(audience_snapshot) }}),current_date()))) as date

),

--macros/stitch_sfmc_audience_transaction/staging/stg_stitch_sfmc_donor_audience_calculated_with_date_spine.sql



stg_stitch_sfmc_donor_audience_calculated_with_date_spine as (

SELECT
    transaction_date_day,
    person_id,
    donor_audience
FROM {{ ref(calculated_audience) }} calc_audience
JOIN stg_stitch_sfmc_audience_transaction_calculated_date_spine date_spine ON date_spine.date = calc_audience.transaction_date_day
WHERE calc_audience.transaction_date_day < (SELECT MAX(date) FROM stg_stitch_sfmc_audience_transaction_calculated_date_spine)

),


-- macros/stitch_sfmc_audience_transaction/staging/stg_stitch_sfmc_donor_audience_calculated_scd.sql

WITH changes AS (
  SELECT
    person_id,
    transaction_date_day,
    donor_audience,
    LAG(donor_audience) OVER (PARTITION BY person_id ORDER BY transaction_date_day) AS prev_donor_audience
  FROM stg_stitch_sfmc_donor_audience_calculated_with_date_spine
),

stg_stitch_sfmc_donor_audience_calculated_scd as (
SELECT
  person_id,
  MIN(transaction_date_day) AS start_date,
  IFNULL(MAX(next_date) -  1, (SELECT MAX(date) FROM stg_stitch_sfmc_audience_transaction_calculated_date_spine)) AS end_date,
  donor_audience
FROM (
  SELECT
    person_id,
    transaction_date_day,
    donor_audience,
    LEAD(transaction_date_day) OVER (PARTITION BY person_id ORDER BY transaction_date_day) AS next_date
  FROM changes
  WHERE prev_donor_audience IS NULL OR donor_audience != prev_donor_audience
) filtered_changes
GROUP BY person_id, donor_audience, next_date
ORDER BY person_id, start_date
),


-- macros/stitch_sfmc_arc/staging/stg_stitch_sfmc_arc_calculated_audience_by_date_day.sql

audience_by_date_day as (
    select
        date_spine.date as date_day,
        calc_audience_scd.person_id,
        calc_audience_scd.donor_audience
    from stg_stitch_sfmc_audience_transaction_calculated_date_spine as date_spine
    inner join
        stg_stitch_sfmc_donor_audience_calculated_scd as calc_audience_scd
        on date_spine.date >= date(calc_audience_scd.start_date)
        and (
            date_spine.date <= date(calc_audience_scd.end_date)
            or date(calc_audience_scd.start_date) is null
        )
),
deduplicated_table_scd as (
    select
        date_day,
        person_id,
        donor_audience,
        row_number() over (
            partition by date_day, person_id order by donor_audience
        ) as row_num
    from audience_by_date_day

),

select date_day, person_id, donor_audience
from deduplicated_table_scd
where row_num = 1



{% endmacro %}
