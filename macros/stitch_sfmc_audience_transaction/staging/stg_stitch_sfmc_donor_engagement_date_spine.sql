{% macro create_stg_stitch_sfmc_donor_engagement_date_spine(
    donor_engagement ='stg_stitch_sfmc_donor_engagement_calculated'
) %}



select date
    from
        unnest(
            generate_date_array(
                (
                    select min(transaction_date_day),
                    from {{ ref(donor_engagement) }}
                ),
                ifnull(
                    (select max(transaction_date_day)
                        from {{ ref(donor_engagement) }}),current_date()))) as date



{% endmacro %}