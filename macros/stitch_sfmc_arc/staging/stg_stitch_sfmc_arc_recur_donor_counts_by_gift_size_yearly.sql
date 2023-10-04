{% macro create_stg_stitch_sfmc_arc_recur_donor_counts_by_gift_size_yearly(
    audience_transaction= "stg_stitch_sfmc_arc_audience_union_transaction_joined"

) %}


Select
    last_day(transaction_date_day, Year) As date_day,
    'yearly' As interval_type,
    gift_size_string As gift_size,
    count(Distinct person_id) As donor_counts
From {{ ref(audience_transaction) }}
Where recurring = True
Group By 1, 2, 3
Order By 1 Desc, 3

{% endmacro %}

