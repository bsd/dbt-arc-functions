{% macro create_stg_stitch_sfmc_audience_transactions_enriched_rollup(
    reference_name="stg_stitch_sfmc_parameterized_audience_transactions_enriched"
) %}

    select
        transaction_date_day,
        person_id,
        inbound_channel,
        recurring,
        (case when recurring is true then gift_size_string end) as recurring_gift_size,
        sum(amount) as amounts,
        count(*) as gifts
    from {{ ref(reference_name) }}
    group by 1, 2, 3, 4, 5

{% endmacro %}
