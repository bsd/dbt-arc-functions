--fmt: off
{% macro util_stg_audience_frequency_donor_counts_by_gift_size_interval(
    frequency,
    interval,
    audience_transaction="stg_audience_transactions_and_audience_summary"
) %}
    {% if frequency not in ['recurring', 'onetime'] %}
        {{ exceptions.raise_compiler_error("'frequency' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'recurring' or 'onetime', got " ~ frequency) }}
    {% endif %}
    {% if interval not in ['day', 'month','year'] %}
        {{ exceptions.raise_compiler_error("'interval' argument to util_stg_stitch_sfmc_audience_transaction_frequency_donor_counts_interval must be 'day', 'week', 'month', or 'year', got " ~ interval) }}
    {% endif %}


select
{% if interval == 'day'%}
    transaction_date_day as date_day,
    'daily' as interval_type,
{% elif interval == 'month' %}
    last_day(transaction_date_day, month) as date_day,
    'monthly' as interval_type,
{% elif interval == 'year' %}
    last_day(transaction_date_day, year) as date_day,
    'yearly' as interval_type,
{% endif %}
    channel,
    donor_audience,
    gift_size_string as gift_size,
    count(distinct person_id) as donor_counts
from {{ ref(audience_transaction) }}
where recurring = true
group by 1, 2, 3, 4, 5
        

{% endmacro %}
