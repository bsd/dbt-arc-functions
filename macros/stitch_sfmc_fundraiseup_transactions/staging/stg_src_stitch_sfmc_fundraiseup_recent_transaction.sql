{% macro create_stg_src_stitch_sfmc_fundraiseup_recent_transaction(
    source_name="src_stitch_fundraiseup",
    source_table="recent_transactions"
) %}

    with
        fru as (
            select
                fru_donation_id as revenue_id,
                __initial_market_source_ as initial_market_source,
                email_address as email,
                cast(amount as float64) as amount,
                gift_type,
                safe_cast(transaction_date as datetime) as transaction_date,
                appeal,
                designation,
                safe_cast(substr(sfmc_updatedate, 1, 19) as datetime) as sfmc_insert_dt,
                safe_cast(migrated as bool) migrated,
                bbcrmlookupid as lookup_id,
                safe_cast(
                    substr(sfmc_updatedate, 1, 19) as datetime
                ) as sfmc_updated_dt,
                _sdc_received_at

            from {{source(source_name,source_table)}}

        ),
        fru_ranked as (

            select
                *,
                row_number() over (
                    partition by revenue_id
                    order by transaction_date desc, _sdc_received_at desc nulls last
                ) as row_num
            from fru
        )
    select * except (row_num, _sdc_received_at)
    from fru_ranked
    where row_num = 1 and cast(amount as float64) < 9999
{% endmacro %}
