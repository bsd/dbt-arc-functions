{% macro create_stg_stitch_sfmc_arc_audience_by_date_day(
    date_spine="stg_stitch_sfmc_arc_audience_date_spine",
    audience_snapshot="snp_stitch_sfmc_arc_audience"
) %}

    with
        audience_by_date_day as (
            select
                date_spine.date as date_day,
                audience_snapshot.subscriberkey as person_id,
                audience_snapshot.__donoraudience_ as donor_audience
            from {{ ref(date_spine) }} as date_spine
            inner join
                {{ ref(audience_snapshot) }} as audience_snapshot
                on date_spine.date >= date(
                    cast(
                        concat(
                            substr(audience_snapshot.dbt_valid_from, 0, 22),
                            " America/New_York"
                        ) as timestamp
                    ),
                    "America/New_York"
                )
                and (
                    date_spine.date <= date(
                        cast(
                            concat(
                                substr(audience_snapshot.dbt_valid_to, 0, 22),
                                " America/New_York"
                            ) as timestamp
                        ),
                        "America/New_York"
                    )
                    or date(
                        cast(
                            concat(
                                substr(audience_snapshot.dbt_valid_to, 0, 22),
                                " America/New_York"
                            ) as timestamp
                        ),
                        "America/New_York"
                    )
                    is null
                )
        ),
        deduplicated_table as (
            select
                date_day,
                person_id,
                donor_audience,
                row_number() over (
                    partition by date_day, person_id order by donor_audience
                ) as row_num
            from audience_by_date_day

        )

    select date_day, person_id, donor_audience
    from deduplicated_table
    where row_num = 1

{% endmacro %}
