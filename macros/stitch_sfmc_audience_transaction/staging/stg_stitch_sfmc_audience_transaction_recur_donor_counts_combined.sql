{% macro create_stg_stitch_sfmc_audience_transaction_recur_donor_counts_combined(
    daily="stg_stitch_sfmc_audience_transaction_recur_donor_counts_daily",
    monthly="stg_stitch_sfmc_audience_transaction_recur_donor_counts_monthly",
    yearly="stg_stitch_sfmc_audience_transaction_recur_donor_counts_yearly"
) %}
select *
from {{ ref(daily) }}
union all
select *
from {{ ref(monthly) }}
union all
select *
from {{ ref(yearly) }}

{% endmacro %}
