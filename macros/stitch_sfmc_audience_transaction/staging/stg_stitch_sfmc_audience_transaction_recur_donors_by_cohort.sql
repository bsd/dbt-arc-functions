{% macro create_stg_stitch_sfmc_audience_transaction_recur_donors_by_cohort(
    transactions_table="stg_stitch_sfmc_audience_transaction_with_first_gift_cohort"
) %}

select
    join_month_year_str,
    coalesce(first_gift_join_source, 'Unknown') as first_gift_join_source,
    join_gift_size_string_recur,
    first_gift_donor_audience,
    month_diff_int,
    count(distinct person_id) as unique_donors
from {{ ref(transactions_table) }}
where first_gift_recur_status = true and recurring = true
group by 1, 2, 3, 4, 5

{% endmacro %}
