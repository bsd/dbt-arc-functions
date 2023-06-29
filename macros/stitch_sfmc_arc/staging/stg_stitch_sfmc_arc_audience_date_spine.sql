{% macro stg_stitch_sfmc_arc_audience_date_spine(
    audience_snapshot="snp_stitch_sfmc_arc_audience"
) %}

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

{% endmacro %}
