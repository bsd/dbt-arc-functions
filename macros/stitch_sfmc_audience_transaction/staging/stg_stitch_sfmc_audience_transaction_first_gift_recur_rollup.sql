{% macro create_stg_stitch_sfmc_audience_transaction_first_gift_recur_rollup(
    first_gift="stg_stitch_sfmc_audience_transaction_first_gift"
) %}

select
join_month_year_str,
first_gift_join_source,
join_gift_size_string_recur,
first_gift_donor_audience,
count(distinct person_id) as donors_in_cohort
from {{ ref(first_gift)}}
where first_gift_recur_status = True
group by 1, 2, 3, 4



{% endmacro %}
