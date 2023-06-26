{% macro create_stg_stitch_sfmc_arc_audience_by_date_day(
    date_spine="stg_stitch_sfmc_arc_audience_date_spine",
    audience_snapshot="snp_stitch_sfmc_arc_audience"
) %}

    with
        audience_by_date_day as (
            select
                d.date as date_day,
                s.subscriberkey as person_id,
                s.__donoraudience_ as arc_audience
            from {{ ref(date_spine) }} as d
            inner join
                {{ ref(audience_snapshot) }} as s
                on d.date >= date(
                    cast(
                        concat(
                            substr(s.dbt_valid_from, 0, 22), " America/New_York"
                        ) as timestamp
                    ),
                    "America/New_York"
                )
                and (
                    d.date <= date(
                        cast(
                            concat(
                                substr(s.dbt_valid_to, 0, 22), " America/New_York"
                            ) as timestamp
                        ),
                        "America/New_York"
                    )
                    or date(
                        cast(
                            concat(
                                substr(s.dbt_valid_to, 0, 22), " America/New_York"
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
                arc_audience,
                row_number() over (
                    partition by date_day, person_id order by arc_audience
                ) as row_num
            from audience_by_date_day

        )

    select date_day, person_id, arc_audience
    from deduplicated_table
    where row_num = 1

{% endmacro %}
