{% macro create_stg_stitch_sfmc_arc_recur_donor_counts_by_gift_size_yearly(
    audience_transaction="stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched"
) %}

with
    base as (
        select
            last_day(transaction_date_day, year) as date_day,
            channel,
            donor_audience,
            gift_size_string as gift_size,
            count(distinct person_id) as donor_counts
        from {{ ref(audience_transaction) }}
        where recurring = true
        group by 1, 2, 3, 4
    )

select 'yearly' as interval_type, *
from base

{% endmacro %}
