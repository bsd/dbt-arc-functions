{% macro create_stg_stitch_sfmc_audience_transaction_calculated_date_spine(
    calculated_audience ="stg_stitch_sfmc_audience_transaction_jobs_append",
    audience_snapshot = "snp_stitch_sfmc_arc_audience"
) %}

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

{% endmacro %}