{% macro create_stg_stitch_sfmc_audience_transactions_join(
    jobs="stg_stitch_sfmc_audience_transaction_jobs",
    first_last="stg_stitch_sfmc_audience_transaction_first_last",
    cumulative="stg_stitch_sfmc_audience_transaction_cumulative",
    yoy="stg_stitch_sfmc_audience_transaction_yoy"
) %}

select
    jobs.transaction_date,
    jobs.person_id,
    first_last.first_transaction_date,
    first_last.previous_latest_transaction_date,
    cumulative.cumulative_amount_12_months,
    cumulative.cumulative_amount_24_months,
    cumulative.cumulative_amount_30_days_recur,
    yoy.donated_within_14_months,
    yoy.donated_within_13_months,
    yoy.donated_current_fiscal_year_july_to_june,
    yoy.donated_last_fiscal_year_july_to_june,
    yoy.donated_two_fiscal_years_ago_july_to_june,
    yoy.donated_three_fiscal_years_ago_july_to_june

from {{ ref(jobs) }} jobs
join
    {{ ref(first_last) }} first_last
    on jobs.transaction_date = first_last.transaction_date
    and jobs.person_id = first_last.person_id
join
    {{ ref(cumulative) }} cumulative
    on jobs.transaction_date = cumulative.transaction_date
    and jobs.person_id = cumulative.person_id
join {{ ref(yoy) }} yoy 
    on jobs.transaction_date = yoy.transaction_date
    and jobs.person_id = yoy.person_id
    

{% endmacro %}
