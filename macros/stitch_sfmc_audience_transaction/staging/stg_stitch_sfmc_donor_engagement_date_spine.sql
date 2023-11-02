{% macro create_stg_stitch_sfmc_donor_engagement_date_spine(
    donor_engagement="stg_stitch_sfmc_audience_transaction_person_engagement_with_start_and_end_dates"
) %}

select date
from
    unnest(
        generate_date_array(
            (select min(start_date), from {{ ref(donor_engagement) }}),
            ifnull(
                (select max(start_date) from {{ ref(donor_engagement) }}), current_date()
            )
        )
    ) as date

{% endmacro %}
