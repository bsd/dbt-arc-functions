{% macro create_stg_stitch_sfmc_audience_transaction_onetime_donor_counts_actuals_rollup(
    daily="stg_stitch_sfmc_audience_transaction_onetime_donor_counts_daily",
    monthly="stg_stitch_sfmc_audience_transaction_onetime_donor_counts_monthly",
    yearly="stg_stitch_sfmc_audience_transaction_onetime_donor_counts_yearly"
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
