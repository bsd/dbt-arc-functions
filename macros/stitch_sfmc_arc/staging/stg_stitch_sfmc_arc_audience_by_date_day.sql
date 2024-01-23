{% macro create_stg_stitch_sfmc_arc_audience_by_date_day(
    audience_snapshot="snp_stitch_sfmc_arc_audience"
) %}

with
    date_spine as (

        select date
        from
            unnest(
                generate_date_array(
                    (
                        select
                            min(
                                date(
                                    cast(
                                        concat(
                                            substr(dbt_valid_from, 0, 22),
                                            " America/New_York"
                                        ) as timestamp
                                    ),
                                    "America/New_York"
                                )
                            )
                        from {{ ref(audience_snapshot) }}
                    ),

                    ifnull(
                        (
                            select
                                max(
                                    date(
                                        cast(
                                            concat(
                                                substr(dbt_valid_to, 0, 22),
                                                " America/New_York"
                                            ) as timestamp
                                        ),
                                        "America/New_York"
                                    )
                                )

                            from {{ ref(audience_snapshot) }}
                        ),
                        current_date()
                    )
                )
            ) as date
    ),
    audience_by_date_day as (
        select
            date_spine.date as date_day,
            audience_snapshot.subscriberkey as person_id,
            audience_snapshot.__donoraudience_ as donor_audience
        from date_spine
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

select
    date_day,
    person_id,
    case
        donor_audience
        when 'midlevel'
        then 'Leadership Giving'
        else initcap(donor_audience)
    end as donor_audience,
from deduplicated_table
where row_num = 1

{% endmacro %}
