{% macro create_stg_stitch_sfmc_audience_transactions_enriched(
    reference_name="stg_stitch_sfmc_audience_transactions_summary_unioned"
) %}

    select
        transaction_id,
        person_id,
        transaction_date_day,
        amount,
        inbound_channel,
        (
            case
                when
                    (
                        lower(source_code) like 'email_%'
                        or lower(source_code) like '%bsd%'
                    )
                then 'Email'
                when lower(source_code) like 'web%'
                then 'Web'
                when
                    (lower(source_code) like 'cpc_%' or lower(source_code) like 'sem_%')
                then 'CPC'
                when
                    (
                        lower(source_code) like 'ref_%'
                        or lower(source_code) like 'referral_%'
                    )
                then 'Referral'
                when lower(source_code) like 'organic_%'
                then 'Organic'
                when lower(source_code) like 'paidsocial_%'
                then 'Paid Social'
                when lower(source_code) like 'display%'
                then 'Display'
                when lower(source_code) like 'social%'
                then 'Social'
                when source_code is null
                then 'No Source'
                else inbound_channel
            end
        ) as best_guess_inbound_channel,  -- This is custom logic for UUSA.
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
