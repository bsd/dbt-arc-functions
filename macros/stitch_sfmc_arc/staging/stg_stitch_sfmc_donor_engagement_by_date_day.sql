{% macro create_stg_stitch_sfmc_donor_engagement_by_date_day(
    donor_engagement_scd_table="stg_stitch_sfmc_donor_engagement_scd"
) %}

with date_spine as (
select date
from
    unnest(
        generate_date_array(
            (select min(start_date), from {{ ref(donor_engagement) }}),
            ifnull(
                (select max(start_date) from {{ ref(donor_engagement) }}), current_date()
            )
        )
    ) as date_day

)
        , engagement_by_date_day as (
            select
                date_spine.date_day,
                donor_engagement_scd.person_id,
                donor_engagement_scd.donor_engagement
            from date_spine
            inner join
                {{ ref(donor_engagement_scd_table) }} as donor_engagement_scd
                on date_spine.date >= date(donor_engagement_scd.start_date)
                and (
                    date_spine.date <= date(donor_engagement_scd.end_date)
                    or date(donor_engagement_scd.start_date) is null
                )
        ),
        deduplicated_table as (
            select
                date_day,
                person_id,
                donor_engagement,
                row_number() over (
                    partition by date_day, person_id order by donor_engagement
                ) as row_num
            from engagement_by_date_day

        )

    select date_day, person_id, donor_engagement
    from deduplicated_table
    where row_num = 1

{% endmacro %}
