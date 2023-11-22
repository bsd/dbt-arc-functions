{% macro create_stg_src_stitch_sfmc_bbcrm_transaction() %}

    with
        revenue_historical as (
            select
                bbcrmlookupid,
                constituentsystemrecordid,
                statuscode,
                cast(recordid as string) as recordid,
                revenue_id,
                cast(transaction_date as datetime) as transaction_date,
                payment_method,
                cast(recognition_amount as string) as amount,
                inbound_channel,
                appeal,
                appeal_business_unit,
                initial_market_source,
                web_donation_form_value,
                web_donation_form_comment,
                designation,
                transaction_type,
                application,
                vendor_order_number,
                revenue_platform,
                sfmc_dateadded,
                sfmc_updatedate
            from {{ source("src_uusa_bbcrm_historical", "revenue_historical") }}

        ),

        current_fiscal_historical as (
            select
                bbcrmlookupid,
                constituentsystemrecordid,
                statuscode,
                cast(recordid as string) as recordid,
                revenue_id,
                cast(transaction_date as datetime) as transaction_date,
                payment_method,
                cast(recognition_amount as string) as amount,
                inbound_channel,
                appeal,
                appeal_business_unit,
                initial_market_source,
                web_donation_form_value,
                web_donation_form_comment,
                designation,
                transaction_type,
                application,
                vendor_order_number,
                revenue_platform,
                sfmc_dateadded,
                sfmc_updatedate
            from
                {{ source("src_uusa_bbcrm_historical", "revenue_current_fiscal_hist") }}

        ),

        stitch_current_fiscal as (

            select
                __bbcrmlookupid_ as bbcrmlookupid,
                constituentsystemrecordid as constituentsystemrecordid,
                cast(statuscode as string) as statuscode,
                cast(recordid as string) as recordid,
                revenue_id as revenue_id,
                safe_cast(transaction_date as datetime) as transaction_date,
                payment_method as payment_method,
                cast(recognition_amount as string) as amount,
                inbound_channel as inbound_channel,
                appeal as appeal,
                appeal_business_unit as appeal_business_unit,
                initial_market_source as initial_market_source,
                web_donation_form_value as web_donation_form_value,
                web_donation_form_comment as web_donation_form_comment,
                designation as designation,
                transaction_type as transaction_type,
                application as application,
                vendor_order_number as vendor_order_number,
                revenue_platform as revenue_platform,
                sfmc_dateadded as sfmc_dateadded,
                sfmc_updatedate as sfmc_updatedate

            from {{ source("src_stitch_bbcrm", "revenue") }}
        ),

        current_fiscal_unioned as (

            select *
            from revenue_historical
            union all
            select *
            from current_fiscal_historical
            union all
            select *
            from stitch_current_fiscal
        ),

        current_fiscal_ranked as (

            select
                *,
                row_number() over (
                    partition by revenue_id order by sfmc_updatedate desc
                ) as row_num
            from current_fiscal_unioned

        ),

        final as (
            select * except (row_num)
            from current_fiscal_ranked
            where row_num = 1 and revenue_id != 'rev-44816929'
        )

    select *
    from final
    where cast(transaction_date as datetime) < current_datetime()

{% endmacro %}
