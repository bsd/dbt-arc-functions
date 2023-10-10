{% macro create_stg_stitch_sfmc_parameterized_audience_transactions_enriched(
    best_guess_inbound_channel,
    reference_name="stg_stitch_sfmc_audience_transactions_summary_unioned"
) %}

    select
        transaction_id,
        person_id,
        transaction_date_day,
        amount,
        initcap(inbound_channel) as inbound_channel,
        initcap({{ best_guess_inbound_channel }}) as best_guess_inbound_channel,
        recurring,
        (
            case
                when amount between 0 and 25
                then '0-25'
                when amount between 26 and 100
                then '26-100'
                when amount between 101 and 250
                then '101-250'
                when amount between 251 and 500
                then '251-500'
                when amount between 501 and 1000
                then '501-1000'
                when amount between 1001 and 10000
                then '1001-10000'
                else '10000+'
            end
        ) as gift_size_string,
        row_number() over (
            partition by person_id order by transaction_date_day
        ) as gift_count
    from {{ ref(reference_name) }}

{% endmacro %}
