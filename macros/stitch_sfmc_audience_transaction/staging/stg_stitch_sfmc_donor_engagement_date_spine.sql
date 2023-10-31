{% macro create_stg_stitch_sfmc_donor_engagement_date_spine(
    donor_engagement="stg_stitch_sfmc_audience_transaction_person_with_donor_engagement"
) %}

select date
from
    unnest(
        generate_date_array(
            (select min(date_day), from {{ ref(donor_engagement) }}),
            ifnull(
                (select max(date_day) from {{ ref(donor_engagement) }}), current_date()
            )
        )
    ) as date

{% endmacro %}
